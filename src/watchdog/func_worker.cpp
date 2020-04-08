#include "watchdog/func_worker.h"

#include "common/time.h"
#include "utils/fs.h"
#include "watchdog/func_runner.h"
#include "watchdog/watchdog.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace watchdog {

using protocol::MessageType;
using protocol::Message;

constexpr size_t FuncWorker::kWriteBufferSize;

FuncWorker::FuncWorker(Watchdog* watchdog, int worker_id, bool async)
    : state_(kCreated), watchdog_(watchdog), worker_id_(worker_id), async_(async),
      log_header_(absl::StrFormat("FuncWorker[%d]: ", worker_id)),
      subprocess_(watchdog->fprocess()), inflight_requests_(0) {}

FuncWorker::~FuncWorker() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void FuncWorker::Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool) {
    DCHECK(state_ == kCreated);
    uv_loop_ = uv_loop;
    read_buffer_pool_ = read_buffer_pool;
    input_pipe_fd_ = subprocess_.CreateReadablePipe();
    output_pipe_fd_ = subprocess_.CreateWritablePipe();
    subprocess_.AddEnvVariable("GATEWAY_IPC_PATH", fs_utils::GetRealPath(watchdog_->gateway_ipc_path()));
    subprocess_.AddEnvVariable("FUNC_ID", watchdog_->func_id());
    subprocess_.AddEnvVariable("INPUT_PIPE_FD", input_pipe_fd_);
    subprocess_.AddEnvVariable("OUTPUT_PIPE_FD", output_pipe_fd_);
    subprocess_.AddEnvVariable("SHARED_MEMORY_PATH", fs_utils::GetRealPath(watchdog_->shared_mem_path()));
    subprocess_.AddEnvVariable("FUNC_CONFIG_FILE", fs_utils::GetRealPath(watchdog_->func_config_file()));
    subprocess_.AddEnvVariable("WORKER_ID", worker_id_);
    subprocess_.AddEnvVariable("GOMAXPROCS", watchdog_->go_max_procs());
    if (!watchdog_->func_worker_output_dir().empty()) {
        std::string_view output_dir = watchdog_->func_worker_output_dir();
        std::string_view func_name = watchdog_->func_name();
        subprocess_.SetStandardFile(Subprocess::kStdout,
                                    absl::StrFormat("%s/%s_worker_%d.stdout",
                                                    output_dir, func_name, worker_id_));
        subprocess_.SetStandardFile(Subprocess::kStderr,
                                    absl::StrFormat("%s/%s_worker_%d.stderr",
                                                    output_dir, func_name, worker_id_));
    }
    if (!watchdog_->fprocess_working_dir().empty()) {
        subprocess_.SetWorkingDir(watchdog_->fprocess_working_dir());
    }
    CHECK(subprocess_.Start(uv_loop, read_buffer_pool,
                            absl::bind_front(&FuncWorker::OnSubprocessExit, this)))
        << "Failed to start fprocess";
    uv_input_pipe_handle_ = subprocess_.GetPipe(input_pipe_fd_);
    uv_input_pipe_handle_->data = this;
    uv_output_pipe_handle_ = subprocess_.GetPipe(output_pipe_fd_);
    uv_output_pipe_handle_->data = this;
    if (async_) {
        HLOG(INFO) << "Enable async mode";
        state_ = kAsync;
        write_buffer_pool_ = std::make_unique<utils::BufferPool>(
            absl::StrFormat("FuncWorker[%d]", worker_id_), kWriteBufferSize);
        write_req_pool_ = std::make_unique<utils::SimpleObjectPool<uv_write_t>>();
        UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(uv_output_pipe_handle_),
                                   &FuncWorker::BufferAllocCallback,
                                   &FuncWorker::ReadMessageCallback));
    } else {
        state_ = kIdle;
        watchdog_->OnFuncWorkerIdle(this);
    }
}

void FuncWorker::ScheduleClose() {
    DCHECK(state_ != kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
    if (state_ == kClosed || state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled to close or has closed";
        return;
    }
    state_ = kClosing;
    subprocess_.Kill();
}

bool FuncWorker::ScheduleFuncCall(WorkerFuncRunner* func_runner, uint64_t call_id) {
    DCHECK(state_ != kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
    if (state_ == kClosed || state_ == kClosing) {
        HLOG(WARNING) << "This FuncWorker is scheduled to close or already closed, "
                      << "cannot schedule function call further";
        return false;
    }
    func_runners_[call_id] = func_runner;
    if (state_ == kAsync || state_ == kIdle) {
        DispatchFuncCall(call_id);
    } else {
        pending_func_calls_.push(call_id);
    }
    return true;
}

void FuncWorker::OnSubprocessExit(int exit_status, std::span<const char> stdout,
                                  std::span<const char> stderr) {
    DCHECK(state_ != kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
    if (exit_status != 0) {
        HLOG(WARNING) << "Subprocess exits abnormally with code: " << exit_status;
    }
    if (stderr.size() > 0) {
        HVLOG(1) << "Stderr: " << std::string_view(stderr.data(), stderr.size());
    }
    for (const auto& entry : func_runners_) {
        WorkerFuncRunner* func_runner = entry.second;
        func_runner->Complete(FuncRunner::kProcessExitAbnormally);
    }
    func_runners_.clear();
    state_ = kClosed;
    watchdog_->OnFuncWorkerClose(this);
}

void FuncWorker::OnRecvMessage(const Message& message) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
#ifdef __FAAS_ENABLE_PROFILING
    watchdog_->func_worker_message_delay_stat()->AddSample(
        GetMonotonicMicroTimestamp() - message.send_timestamp);
#endif
    MessageType type = static_cast<MessageType>(message.message_type);
    uint64_t call_id = message.func_call.full_call_id;
    if (type == MessageType::FUNC_CALL_COMPLETE || type == MessageType::FUNC_CALL_FAILED) {
        if (func_runners_.contains(call_id)) {
            WorkerFuncRunner* func_runner = func_runners_[call_id];
            if (type == MessageType::FUNC_CALL_COMPLETE) {
#ifdef __FAAS_ENABLE_PROFILING
                func_runner->Complete(FuncRunner::kSuccess, message.processing_time);
#else
                func_runner->Complete(FuncRunner::kSuccess);
#endif
            } else {
                func_runner->Complete(FuncRunner::kFailedWithoutReason);
            }
            func_runners_.erase(call_id);
        } else {
            HLOG(ERROR) << "Cannot find function runner for call_id " << call_id;
        }
    }
    if (state_ == kClosed || state_ == kClosing) {
        HLOG(WARNING) << "This FuncWorker is scheduled to close or already closed";
        return;
    }
    if (state_ != kAsync) {
        DCHECK(state_ == kReceiving);
        state_ = kIdle;
        if (!pending_func_calls_.empty()) {
            uint64_t call_id = pending_func_calls_.front();
            pending_func_calls_.pop();
            DispatchFuncCall(call_id);
        } else {
            watchdog_->OnFuncWorkerIdle(this);
        }
    } else {
        inflight_requests_--;
    }
}

void FuncWorker::DispatchFuncCall(uint64_t call_id) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
    if (state_ == kClosed || state_ == kClosing) {
        HLOG(WARNING) << "This FuncWorker is scheduled to close or already closed, "
                      << "cannot dispatch function call further";
        return;
    }
    DCHECK(state_ == kAsync || state_ == kIdle);
    if (subprocess_.PipeClosed(input_pipe_fd_)) {
        HLOG(WARNING) << "Input pipe closed, cannot send data further";
        return;
    }
    Message* message;
    uv_write_t* write_req;
    if (state_ == kAsync) {
        char* buf;
        size_t size;
        write_buffer_pool_->Get(&buf, &size);
        message = reinterpret_cast<Message*>(buf);
        write_req = write_req_pool_->Get();
        write_req->data = buf;
    } else {
        message = &message_to_send_;
        write_req = &write_req_;
    }
    message->message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC);
    message->func_call.full_call_id = call_id;
#ifdef __FAAS_ENABLE_PROFILING
    message->send_timestamp = GetMonotonicMicroTimestamp();
#endif
    uv_buf_t buf = {
        .base = reinterpret_cast<char*>(message),
        .len = sizeof(Message)
    };
    if (state_ == kIdle) {
        state_ = kSending;
    } else {
        inflight_requests_++;
    }
    UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(uv_input_pipe_handle_),
                          &buf, 1, &FuncWorker::WriteMessageCallback));
}

UV_ALLOC_CB_FOR_CLASS(FuncWorker, BufferAlloc) {
    read_buffer_pool_->Get(buf);
}

UV_READ_CB_FOR_CLASS(FuncWorker, ReadMessage) {
    auto reclaim_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            read_buffer_pool_->Return(buf);
        }
    });
    if (nread < 0) {
        HLOG(WARNING) << "Read error, will close this FuncWorker: "
                      << uv_strerror(nread);
        ScheduleClose();
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    if (state_ == kAsync) {
        utils::ReadMessages<Message>(
            &recv_buffer_, buf->base, nread,
            [this] (Message* message) {
                OnRecvMessage(*message);
            });
    } else {
        DCHECK(state_ == kReceiving);
        recv_buffer_.AppendData(buf->base, nread);
        if (recv_buffer_.length() == sizeof(Message)) {
            if (!subprocess_.PipeClosed(output_pipe_fd_)) {
                UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(uv_output_pipe_handle_)));
            }
            Message* message = reinterpret_cast<Message*>(recv_buffer_.data());
            OnRecvMessage(*message);
        } else if (recv_buffer_.length() > sizeof(Message)) {
            HLOG(ERROR) << "Read more data than expected, will close this FuncWorker";
            ScheduleClose();
        }
    }
}

UV_WRITE_CB_FOR_CLASS(FuncWorker, WriteMessage) {
    auto reclaim_resource = gsl::finally([this, req] {
        if (state_ == kAsync) {
            write_buffer_pool_->Return(reinterpret_cast<char*>(req->data));
            write_req_pool_->Return(req);
        }
    });
    if (status != 0) {
        HLOG(WARNING) << "Failed to write request, will close this FuncWorker: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    if (subprocess_.PipeClosed(output_pipe_fd_)) {
        HLOG(WARNING) << "Output pipe closed, cannot read response further";
        return;
    }
    if (state_ != kAsync) {
        state_ = kReceiving;
        recv_buffer_.Reset();
        UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(uv_output_pipe_handle_),
                                   &FuncWorker::BufferAllocCallback,
                                   &FuncWorker::ReadMessageCallback));
    }
}

}  // namespace watchdog
}  // namespace faas
