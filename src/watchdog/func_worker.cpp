#include "watchdog/func_worker.h"

#include "watchdog/watchdog.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace watchdog {

FuncWorker::FuncWorker(Watchdog* watchdog, int worker_id, 
                       RunMode run_mode, absl::string_view fprocess)
    : watchdog_(watchdog), worker_id_(worker_id), state_(kCreated), run_mode_(run_mode),
      fprocess_(fprocess), exit_status_(-1), closed_uv_handles_(0),
      log_header_(absl::StrFormat("FuncWorker[%d]: ", worker_id)),
      stdin_eof_(false) {}

FuncWorker::~FuncWorker() {
    CHECK(state_ == kCreated || state_ == kExited);
}

void FuncWorker::BuildReadablePipe(uv_loop_t* uv_loop, uv_pipe_t* uv_pipe,
                                   uv_stdio_container_t* stdio_container) {
    UV_CHECK_OK(uv_pipe_init(uv_loop, uv_pipe, 0));
    uv_pipe->data = this;
    stdio_container->flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_READABLE_PIPE);
    stdio_container->data.stream = reinterpret_cast<uv_stream_t*>(uv_pipe);
}

void FuncWorker::BuildWritablePipe(uv_loop_t* uv_loop, uv_pipe_t* uv_pipe,
                                   uv_stdio_container_t* stdio_container) {
    UV_CHECK_OK(uv_pipe_init(uv_loop, uv_pipe, 0));
    uv_pipe->data = this;
    stdio_container->flags = static_cast<uv_stdio_flags>(UV_CREATE_PIPE | UV_WRITABLE_PIPE);
    stdio_container->data.stream = reinterpret_cast<uv_stream_t*>(uv_pipe);
}

void FuncWorker::Start(uv_loop_t* uv_loop, utils::BufferPool* buffer_pool) {
    CHECK(run_mode_ == RunMode::SERIALIZING) << "Unsupported run mode";
    buffer_pool_ = buffer_pool;
    uv_process_options_t options;
    memset(&options, 0, sizeof(uv_process_options_t));
    options.exit_cb = &FuncWorker::ProcessExitCallback;
    options.file = fprocess_.c_str();
    char* args[] = { const_cast<char*>(fprocess_.c_str()), nullptr };
    options.args = args;
    uv_stdio_container_t stdio[3];
    BuildReadablePipe(uv_loop, &uv_stdin_pipe_, &stdio[0]);
    BuildWritablePipe(uv_loop, &uv_stdout_pipe_, &stdio[1]);
    BuildWritablePipe(uv_loop, &uv_stderr_pipe_, &stdio[2]);
    options.stdio_count = 3;
    options.stdio = stdio;
    uv_process_handle_.data = this;
    UV_CHECK_OK(uv_spawn(uv_loop, &uv_process_handle_, &options));
    UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_stdout_pipe_),
                              &FuncWorker::BufferAllocCallback, &FuncWorker::ReadStdoutCallback));
    UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_stderr_pipe_),
                              &FuncWorker::BufferAllocCallback, &FuncWorker::ReadStderrCallback));
    total_uv_handles_ = 4;
}

void FuncWorker::ScheduleExit() {
    if (state_ != kRunning) {
        HLOG(INFO) << "Process not in running state, no need to kill";
        return;
    }
    UV_CHECK_OK(uv_process_kill(&uv_process_handle_, SIGKILL));
}

void FuncWorker::CopyRecvData(ssize_t nread, const uv_buf_t* inbuf,
                              utils::AppendableBuffer* outbuf) {
    if (nread < 0) {
        HLOG(WARNING) << "Read error, will exit the process: " << uv_strerror(nread);
        ScheduleExit();
    } else if (nread > 0) {
        outbuf->AppendData(inbuf->base, nread);
    }
    if (inbuf->base != 0) {
        buffer_pool_->Return(inbuf);
    }
}

void FuncWorker::WriteToStdin(const char* data, size_t length,
                              std::function<void(int)> callback) {
    uv_write_t* write_req = new uv_write_t;
    std::function<void(int)>* fn_ptr = new std::function<void(int)>;
    *fn_ptr = callback;
    write_req->data = fn_ptr;
    uv_buf_t buf = { .base = const_cast<char*>(data), .len = length };
    UV_CHECK_OK(uv_write(write_req, reinterpret_cast<uv_stream_t*>(&uv_stdin_pipe_),
                         &buf, 1, &FuncWorker::WriteStdinCallback));
}

void FuncWorker::StdinEof() {
    if (stdin_eof_) return;
    stdin_eof_ = true;
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_stdin_pipe_), &FuncWorker::CloseCallback);
}

UV_ALLOC_CB_FOR_CLASS(FuncWorker, BufferAlloc) {
    buffer_pool_->Get(buf);
}

UV_READ_CB_FOR_CLASS(FuncWorker, ReadStdout) {
    CopyRecvData(nread, buf, &stdout_);
}

UV_READ_CB_FOR_CLASS(FuncWorker, ReadStderr) {
    CopyRecvData(nread, buf, &stderr_);
}

UV_WRITE_CB_FOR_CLASS(FuncWorker, WriteStdin) {
    std::function<void(int)>* fn_ptr = reinterpret_cast<std::function<void(int)>*>(req->data);
    delete req;
    (*fn_ptr)(status);
    delete fn_ptr;
    if (status != 0) {
        ScheduleExit();
    }
}

UV_EXIT_CB_FOR_CLASS(FuncWorker, ProcessExit) {
    exit_status_ = static_cast<int>(exit_status);
    if (!stdin_eof_) {
        stdin_eof_ = true;
        uv_close(reinterpret_cast<uv_handle_t*>(&uv_stdin_pipe_), &FuncWorker::CloseCallback);
    }
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_stdout_pipe_), &FuncWorker::CloseCallback);
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_stderr_pipe_), &FuncWorker::CloseCallback);
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_process_handle_), &FuncWorker::CloseCallback);
    state_ = kExited;
}

UV_CLOSE_CB_FOR_CLASS(FuncWorker, Close) {
    closed_uv_handles_++;
    if (closed_uv_handles_ == total_uv_handles_) {
        watchdog_->OnFuncWorkerExit(this);
    }
}

}  // namespace watchdog
}  // namespace faas
