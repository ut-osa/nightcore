#include "watchdog/subprocess.h"

#define HLOG(l) LOG(l) << "Subprocess: "
#define HVLOG(l) VLOG(l) << "Subprocess: "

namespace faas {
namespace watchdog {

Subprocess::Subprocess(absl::string_view cmd, size_t max_stdout_size, size_t max_stderr_size)
    : state_(kCreated), cmd_(cmd),
      max_stdout_size_(max_stdout_size), max_stderr_size_(max_stderr_size) {
    pipe_types_.push_back(UV_READABLE_PIPE);  // stdin
    pipe_types_.push_back(UV_WRITABLE_PIPE);  // stdout
    pipe_types_.push_back(UV_WRITABLE_PIPE);  // stderr
    std_fds_.assign(kNumStdPipes, -1);
}

Subprocess::~Subprocess() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void Subprocess::SetStandardFile(StandardPipe pipe, absl::string_view file_path) {
    DCHECK(state_ == kCreated);
    DCHECK_EQ(std_fds_[pipe], -1);
    int fd;
    if (pipe == kStdin) {
        fd = open(std::string(file_path).c_str(), O_RDONLY);
        PCHECK(fd != -1);
    } else {
        fd = creat(std::string(file_path).c_str(), 0666);
        PCHECK(fd != -1);
    }
    std_fds_[pipe] = fd;
}

int Subprocess::CreateReadablePipe() {
    DCHECK(state_ == kCreated);
    pipe_types_.push_back(UV_READABLE_PIPE);
    return static_cast<int>(pipe_types_.size()) - 1;
}

int Subprocess::CreateWritablePipe() {
    DCHECK(state_ == kCreated);
    pipe_types_.push_back(UV_WRITABLE_PIPE);
    return static_cast<int>(pipe_types_.size()) - 1;
}

void Subprocess::AddEnvVariable(absl::string_view name, absl::string_view value) {
    DCHECK(state_ == kCreated);
    env_variables_.push_back(absl::StrFormat("%s=%s", name, value));
}

void Subprocess::AddEnvVariable(absl::string_view name, int value) {
    DCHECK(state_ == kCreated);
    env_variables_.push_back(absl::StrFormat("%s=%d", name, value));
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
    std::vector<const char*> env_ptrs;
    // First add all parent environment variables
    char** ptr = environ;
    while (*ptr != nullptr) {
        env_ptrs.push_back(*ptr);
        ptr++;
    }
    // Then add new variables
    for (const std::string& item : env_variables_) {
        env_ptrs.push_back(item.c_str());
    }
    env_ptrs.push_back(nullptr);
    options.env = const_cast<char**>(env_ptrs.data());
    int num_pipes = pipe_types_.size();
    DCHECK_GE(num_pipes, 3);
    std::vector<uv_stdio_container_t> stdio(num_pipes);
    uv_pipe_handles_.resize(num_pipes);
    pipe_closed_.assign(num_pipes, false);
    closed_uv_handles_ = 0;
    total_uv_handles_ = 1;
    for (int i = 0; i < num_pipes; i++) {
        uv_pipe_t* uv_pipe = &uv_pipe_handles_[i];
        if (i < kNumStdPipes && std_fds_[i] != -1) {
            stdio[i].flags = UV_INHERIT_FD;
            stdio[i].data.fd = std_fds_[i];
            pipe_closed_[i] = true;
        } else {
            UV_DCHECK_OK(uv_pipe_init(uv_loop, uv_pipe, 0));
            uv_pipe->data = this;
            stdio[i].flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | pipe_types_[i]);
            stdio[i].data.stream = UV_AS_STREAM(uv_pipe);
            total_uv_handles_++;
        }
    }
    options.stdio_count = num_pipes;
    options.stdio = stdio.data();
    uv_process_handle_.data = this;
    if (uv_spawn(uv_loop, &uv_process_handle_, &options) != 0) {
        return false;
    }
    if (std_fds_[kStdout] == -1) {
        UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_pipe_handles_[kStdout]),
                                   &Subprocess::BufferAllocCallback, &Subprocess::ReadStdoutCallback));
    }
    if (std_fds_[kStderr] == -1) {
        UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_pipe_handles_[kStderr]),
                                   &Subprocess::BufferAllocCallback, &Subprocess::ReadStderrCallback));
    }
    for (int i = 0; i < kNumStdPipes; i++) {
        if (std_fds_[i] != -1) {
            PCHECK(close(std_fds_[i]) == 0);
        }
    }
    state_ = kRunning;
    return true;
}

void Subprocess::Kill(int signum) {
    DCHECK(state_ != kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_process_handle_.loop);
    if (state_ == kRunning) {
        UV_DCHECK_OK(uv_process_kill(&uv_process_handle_, signum));
    } else {
        HLOG(WARNING) << "Process not in running state, cannot kill";
    }
}

uv_pipe_t* Subprocess::GetPipe(int fd) {
    DCHECK(state_ != kCreated);
    // We prevent touching stdout and stderr pipes directly
    DCHECK(fd != kStdout && fd != kStderr);
    return &uv_pipe_handles_[fd];
}

void Subprocess::ClosePipe(int fd) {
    DCHECK(state_ != kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_process_handle_.loop);
    if (pipe_closed_[fd]) {
        return;
    }
    pipe_closed_[fd] = true;
    uv_pipe_t* uv_pipe = &uv_pipe_handles_[fd];
    uv_pipe->data = this;
    uv_close(UV_AS_HANDLE(uv_pipe), &Subprocess::CloseCallback);
}

bool Subprocess::PipeClosed(int fd) {
    DCHECK(state_ != kCreated);
    return pipe_closed_[fd];
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
        } else {
            ClosePipe(kStdout);
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
        } else {
            ClosePipe(kStderr);
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
    for (size_t i = 0; i < uv_pipe_handles_.size(); i++) {
        ClosePipe(i);
    }
    uv_close(UV_AS_HANDLE(&uv_process_handle_), &Subprocess::CloseCallback);
    state_ = kExited;
}

UV_CLOSE_CB_FOR_CLASS(Subprocess, Close) {
    DCHECK_LT(closed_uv_handles_, total_uv_handles_);
    closed_uv_handles_++;
    if (closed_uv_handles_ == total_uv_handles_) {
        state_ = kClosed;
        exit_callback_(exit_status_, stdout_.to_span(), stderr_.to_span());
    }
}

}  // namespace watchdog
}  // namespace faas
