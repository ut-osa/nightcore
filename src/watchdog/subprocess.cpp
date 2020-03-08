#include "watchdog/subprocess.h"

#define HLOG(l) LOG(l) << "Subprocess: "
#define HVLOG(l) VLOG(l) << "Subprocess: "

namespace faas {
namespace watchdog {

Subprocess::Subprocess(absl::string_view cmd, size_t max_stdout_size, size_t max_stderr_size)
    : state_(kCreated), cmd_(cmd),
      max_stdout_size_(max_stdout_size), max_stderr_size_(max_stderr_size),
      stdin_pipe_closed_(false) {}

Subprocess::~Subprocess() {
    CHECK(state_ == kCreated || state_ == kClosed);
}

void Subprocess::BuildReadablePipe(uv_loop_t* uv_loop, uv_pipe_t* uv_pipe,
                                   uv_stdio_container_t* stdio_container) {
    UV_CHECK_OK(uv_pipe_init(uv_loop, uv_pipe, 0));
    uv_pipe->data = this;
    stdio_container->flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_READABLE_PIPE);
    stdio_container->data.stream = UV_AS_STREAM(uv_pipe);
}

void Subprocess::BuildWritablePipe(uv_loop_t* uv_loop, uv_pipe_t* uv_pipe,
                                   uv_stdio_container_t* stdio_container) {
    UV_CHECK_OK(uv_pipe_init(uv_loop, uv_pipe, 0));
    uv_pipe->data = this;
    stdio_container->flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_WRITABLE_PIPE);
    stdio_container->data.stream = UV_AS_STREAM(uv_pipe);
}

bool Subprocess::Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool,
                       ExitCallback exit_callback) {
    read_buffer_pool_ = read_buffer_pool;
    exit_callback_ = exit_callback;
    uv_process_options_t options;
    memset(&options, 0, sizeof(uv_process_options_t));
    options.exit_cb = &Subprocess::ProcessExitCallback;
    options.file = kShellPath;
    const char* args[] = { kShellPath, "-c", cmd_.c_str(), nullptr };
    options.args = const_cast<char**>(args);
    uv_stdio_container_t stdio[3];
    BuildReadablePipe(uv_loop, &uv_stdin_pipe_, &stdio[0]);
    BuildWritablePipe(uv_loop, &uv_stdout_pipe_, &stdio[1]);
    BuildWritablePipe(uv_loop, &uv_stderr_pipe_, &stdio[2]);
    options.stdio_count = 3;
    options.stdio = stdio;
    uv_process_handle_.data = this;
    if (uv_spawn(uv_loop, &uv_process_handle_, &options) != 0) {
        return false;
    }
    closed_uv_handles_ = 0;
    total_uv_handles_ = 4;
    UV_CHECK_OK(uv_read_start(UV_AS_STREAM(&uv_stdout_pipe_),
                              &Subprocess::BufferAllocCallback, &Subprocess::ReadStdoutCallback));
    UV_CHECK_OK(uv_read_start(UV_AS_STREAM(&uv_stderr_pipe_),
                              &Subprocess::BufferAllocCallback, &Subprocess::ReadStderrCallback));
    state_ = kRunning;
    return true;
}

void Subprocess::Kill(int signum) {
    CHECK(state_ != kCreated);
    CHECK_IN_EVENT_LOOP_THREAD(uv_process_handle_.loop);
    if (state_ == kRunning) {
        UV_CHECK_OK(uv_process_kill(&uv_process_handle_, signum));
    } else {
        HLOG(WARNING) << "Process not in running state, cannot kill";
    }
}

uv_pipe_t* Subprocess::StdinPipe() {
    CHECK(state_ != kCreated);
    CHECK_IN_EVENT_LOOP_THREAD(uv_process_handle_.loop);
    return &uv_stdin_pipe_;
}

void Subprocess::CloseStdin() {
    CHECK(state_ != kCreated);
    CHECK_IN_EVENT_LOOP_THREAD(uv_process_handle_.loop);
    if (stdin_pipe_closed_) {
        return;
    }
    stdin_pipe_closed_ = true;
    uv_stdin_pipe_.data = this;
    uv_close(UV_AS_HANDLE(&uv_stdin_pipe_), &Subprocess::CloseCallback);
}

UV_ALLOC_CB_FOR_CLASS(Subprocess, BufferAlloc) {
    read_buffer_pool_->Get(buf);
}

UV_READ_CB_FOR_CLASS(Subprocess, ReadStdout) {
    if (nread < 0) {
        if (nread != UV_EOF) {
            HLOG(WARNING) << "Read error on stdout, will kill the process: "
                          << uv_strerror(nread);
            Kill();
        }
    } else if (nread > 0) {
        if (stdout_.length() + nread > max_stdout_size_) {
            HLOG(WARNING) << "Exceed stdout size limit, will kill the process";
            Kill();
        } else {
            stdout_.AppendData(buf->base, nread);
        }
    }
    if (buf->base != 0) {
        read_buffer_pool_->Return(buf);
    }
}

UV_READ_CB_FOR_CLASS(Subprocess, ReadStderr) {
    if (nread < 0) {
        if (nread != UV_EOF) {
            HLOG(WARNING) << "Read error on stderr, will kill the process: "
                          << uv_strerror(nread);
            Kill();
        }
    } else if (nread > 0) {
        if (stderr_.length() + nread > max_stderr_size_) {
            HLOG(WARNING) << "Exceed stderr size limit, will kill the process";
            Kill();
        } else {
            stderr_.AppendData(buf->base, nread);
        }
    }
    if (buf->base != 0) {
        read_buffer_pool_->Return(buf);
    }
}

UV_EXIT_CB_FOR_CLASS(Subprocess, ProcessExit) {
    exit_status_ = static_cast<int>(exit_status);
    CloseStdin();
    uv_close(UV_AS_HANDLE(&uv_stdout_pipe_), &Subprocess::CloseCallback);
    uv_close(UV_AS_HANDLE(&uv_stderr_pipe_), &Subprocess::CloseCallback);
    uv_close(UV_AS_HANDLE(&uv_process_handle_), &Subprocess::CloseCallback);
    state_ = kExited;
}

UV_CLOSE_CB_FOR_CLASS(Subprocess, Close) {
    CHECK_LT(closed_uv_handles_, total_uv_handles_);
    closed_uv_handles_++;
    if (closed_uv_handles_ == total_uv_handles_) {
        state_ = kClosed;
        exit_callback_(exit_status_, stdout_.to_span(), stderr_.to_span());
    }
}

}  // namespace watchdog
}  // namespace faas
