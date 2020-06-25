#include "common/subprocess.h"

#define HLOG(l) LOG(l) << "Subprocess: "
#define HVLOG(l) VLOG(l) << "Subprocess: "

namespace faas {
namespace uv {

Subprocess::Subprocess(std::string_view cmd, size_t max_stdout_size, size_t max_stderr_size)
    : state_(kCreated), cmd_(cmd),
      max_stdout_size_(max_stdout_size), max_stderr_size_(max_stderr_size),
      pid_(-1) {
    pipe_types_.push_back(UV_READABLE_PIPE);  // stdin
    pipe_types_.push_back(UV_WRITABLE_PIPE);  // stdout
    pipe_types_.push_back(UV_WRITABLE_PIPE);  // stderr
    std_fds_.assign(kNumStdPipes, -1);
}

Subprocess::~Subprocess() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void Subprocess::SetStandardFile(StandardPipe pipe, std::string_view file_path) {
    DCHECK(state_ == kCreated);
    DCHECK_EQ(std_fds_[pipe], -1);
    int fd;
    if (pipe == kStdin) {
        fd = open(std::string(file_path).c_str(), O_RDONLY);
        PCHECK(fd != -1);
    } else {
        fd = creat(std::string(file_path).c_str(), __FAAS_FILE_CREAT_MODE);
        PCHECK(fd != -1);
    }
    std_fds_[pipe] = fd;
}

int Subprocess::CreateReadablePipe() {
    DCHECK(state_ == kCreated);
    pipe_types_.push_back(UV_READABLE_PIPE);
    return gsl::narrow_cast<int>(pipe_types_.size()) - 1;
}

int Subprocess::CreateWritablePipe() {
    DCHECK(state_ == kCreated);
    pipe_types_.push_back(UV_WRITABLE_PIPE);
    return gsl::narrow_cast<int>(pipe_types_.size()) - 1;
}

void Subprocess::SetWorkingDir(std::string_view path) {
    DCHECK(state_ == kCreated);
    working_dir_ = std::string(path);
}

void Subprocess::AddEnvVariable(std::string_view name, std::string_view value) {
    DCHECK(state_ == kCreated);
    env_variables_.push_back(fmt::format("{}={}", name, value));
}

void Subprocess::AddEnvVariable(std::string_view name, int value) {
    DCHECK(state_ == kCreated);
    env_variables_.push_back(fmt::format("{}={}", name, value));
}

bool Subprocess::Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool,
                       ExitCallback exit_callback) {
    read_buffer_pool_ = read_buffer_pool;
    exit_callback_ = exit_callback;
    handle_scope_.Init(uv_loop, absl::bind_front(&Subprocess::OnAllHandlesClosed, this));
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
    if (!working_dir_.empty()) {
        options.cwd = working_dir_.c_str();
    }
    int num_pipes = pipe_types_.size();
    DCHECK_GE(num_pipes, 3);
    std::vector<uv_stdio_container_t> stdio(num_pipes);
    uv_pipe_handles_.resize(num_pipes);
    pipe_closed_.assign(num_pipes, false);
    for (int i = 0; i < num_pipes; i++) {
        uv_pipe_t* uv_pipe = &uv_pipe_handles_[i];
        if (i < kNumStdPipes && std_fds_[i] != -1) {
            stdio[i].flags = UV_INHERIT_FD;
            stdio[i].data.fd = std_fds_[i];
            pipe_closed_[i] = true;
        } else {
            UV_DCHECK_OK(uv_pipe_init(uv_loop, uv_pipe, 0));
            uv_pipe->data = this;
            handle_scope_.AddHandle(uv_pipe);
            stdio[i].flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | pipe_types_[i]);
            stdio[i].data.stream = UV_AS_STREAM(uv_pipe);
        }
    }
    options.stdio_count = num_pipes;
    options.stdio = stdio.data();
    uv_process_handle_.data = this;
    if (uv_spawn(uv_loop, &uv_process_handle_, &options) != 0) {
        return false;
    }
    pid_ = uv_process_handle_.pid;
    handle_scope_.AddHandle(&uv_process_handle_);
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
    handle_scope_.CloseHandle(uv_pipe);
}

bool Subprocess::PipeClosed(int fd) {
    DCHECK(state_ != kCreated);
    return pipe_closed_[fd];
}

void Subprocess::OnAllHandlesClosed() {
    DCHECK(state_ == kExited);
    state_ = kClosed;
    exit_callback_(exit_status_, stdout_.to_span(), stderr_.to_span());
}

UV_ALLOC_CB_FOR_CLASS(Subprocess, BufferAlloc) {
    read_buffer_pool_->Get(buf);
}

UV_READ_CB_FOR_CLASS(Subprocess, ReadStdout) {
    auto reclaim_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            read_buffer_pool_->Return(buf);
        }
    });
    if (nread < 0) {
        if (nread != UV_EOF) {
            HLOG(WARNING) << "Read error on stdout, will kill the process: "
                          << uv_strerror(nread);
            Kill();
        } else {
            ClosePipe(kStdout);
        }
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    if (stdout_.length() + nread > max_stdout_size_) {
        HLOG(WARNING) << "Exceed stdout size limit, will kill the process";
        Kill();
    } else {
        stdout_.AppendData(buf->base, nread);
    }
}

UV_READ_CB_FOR_CLASS(Subprocess, ReadStderr) {
    auto reclaim_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            read_buffer_pool_->Return(buf);
        }
    });
    if (nread < 0) {
        if (nread != UV_EOF) {
            HLOG(WARNING) << "Read error on stderr, will kill the process: "
                          << uv_strerror(nread);
            Kill();
        } else {
            ClosePipe(kStderr);
        }
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    if (stderr_.length() + nread > max_stderr_size_) {
        HLOG(WARNING) << "Exceed stderr size limit, will kill the process";
        Kill();
    } else {
        stderr_.AppendData(buf->base, nread);
    }
}

UV_EXIT_CB_FOR_CLASS(Subprocess, ProcessExit) {
    exit_status_ = exit_status;
    for (size_t i = 0; i < uv_pipe_handles_.size(); i++) {
        ClosePipe(i);
    }
    handle_scope_.CloseHandle(&uv_process_handle_);
    state_ = kExited;
}

}  // namespace uv
}  // namespace faas
