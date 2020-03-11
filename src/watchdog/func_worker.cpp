#include "watchdog/func_worker.h"

#include "watchdog/func_runner.h"
#include "watchdog/watchdog.h"

#include <absl/functional/bind_front.h>

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace watchdog {

using protocol::MessageType;
using protocol::Message;

FuncWorker::FuncWorker(Watchdog* watchdog,
                       absl::string_view fprocess, int worker_id)
    : state_(kCreated), watchdog_(watchdog), worker_id_(worker_id),
      log_header_(absl::StrFormat("FuncWorker[%d]: ", worker_id)),
      subprocess_(fprocess) {}

FuncWorker::~FuncWorker() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void FuncWorker::Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool) {
    DCHECK(state_ == kCreated);
    uv_loop_ = uv_loop;
    read_buffer_pool_ = read_buffer_pool;
    input_pipe_fd_ = subprocess_.CreateReadablePipe();
    output_pipe_fd_ = subprocess_.CreateWritablePipe();
    subprocess_.AddEnvVariable("GATEWAY_IPC_PATH", watchdog_->gateway_ipc_path());
    subprocess_.AddEnvVariable("FUNC_ID", watchdog_->func_id());
    subprocess_.AddEnvVariable("INPUT_PIPE_FD", input_pipe_fd_);
    subprocess_.AddEnvVariable("OUTPUT_PIPE_FD", output_pipe_fd_);
    subprocess_.AddEnvVariable("SHARED_MEMORY_PATH", watchdog_->shared_mem_path());
    CHECK(subprocess_.Start(uv_loop, read_buffer_pool,
                            absl::bind_front(&FuncWorker::OnSubprocessExit, this)))
        << "Failed to start fprocess";
    uv_input_pipe_handle_ = subprocess_.GetPipe(input_pipe_fd_);
    uv_input_pipe_handle_->data = this;
    uv_output_pipe_handle_ = subprocess_.GetPipe(output_pipe_fd_);
    uv_output_pipe_handle_->data = this;
    state_ = kIdle;
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
    if (state_ == kIdle) {
        DispatchFuncCall(call_id);
    } else {
        pending_func_calls_.push(call_id);
    }
    return true;
}

void FuncWorker::OnSubprocessExit(int exit_status, absl::Span<const char> stdout,
                                  absl::Span<const char> stderr) {
    DCHECK(state_ != kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
    if (exit_status != 0) {
        HLOG(WARNING) << "Subprocess exits abnormally with code: " << exit_status;
    }
    if (stderr.length() > 0) {
        HVLOG(1) << "Stderr: " << absl::string_view(stderr.data(), stderr.length());
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
    MessageType type = static_cast<MessageType>(message.message_type);
    uint64_t call_id = message.func_call.full_call_id;
    if (type == MessageType::FUNC_CALL_COMPLETE || type == MessageType::FUNC_CALL_FAILED) {
        if (func_runners_.contains(call_id)) {
            WorkerFuncRunner* func_runner = func_runners_[call_id];
            if (type == MessageType::FUNC_CALL_COMPLETE) {
                func_runner->Complete(FuncRunner::kSuccess);
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
    DCHECK(state_ == kReceiving);
    state_ = kIdle;
    if (!pending_func_calls_.empty()) {
        uint64_t call_id = pending_func_calls_.front();
        pending_func_calls_.pop();
        DispatchFuncCall(call_id);
    }
}

void FuncWorker::DispatchFuncCall(uint64_t call_id) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop_);
    if (state_ == kClosed || state_ == kClosing) {
        HLOG(WARNING) << "This FuncWorker is scheduled to close or already closed, "
                      << "cannot dispatch function call further";
        return;
    }
    DCHECK(state_ == kIdle);
    if (subprocess_.PipeClosed(input_pipe_fd_)) {
        HLOG(WARNING) << "Input pipe closed, cannot send data further";
        return;
    }
    message_to_send_.message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC);
    message_to_send_.func_call.full_call_id = call_id;
    uv_buf_t buf = {
        .base = reinterpret_cast<char*>(&message_to_send_),
        .len = sizeof(Message)
    };
    state_ = kSending;
    UV_DCHECK_OK(uv_write(&write_req_, UV_AS_STREAM(uv_input_pipe_handle_),
                         &buf, 1, &FuncWorker::WriteMessageCallback));
}

UV_ALLOC_CB_FOR_CLASS(FuncWorker, BufferAlloc) {
    read_buffer_pool_->Get(buf);
}

UV_READ_CB_FOR_CLASS(FuncWorker, ReadMessage) {
    if (nread < 0) {
        HLOG(WARNING) << "Read error, will close this FuncWorker: "
                      << uv_strerror(nread);
        ScheduleClose();
    } else if (nread > 0) {
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
    if (buf->base != 0) {
        read_buffer_pool_->Return(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(FuncWorker, WriteMessage) {
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
    state_ = kReceiving;
    recv_buffer_.Reset();
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(uv_output_pipe_handle_),
                              &FuncWorker::BufferAllocCallback,
                              &FuncWorker::ReadMessageCallback));
}

}  // namespace watchdog
}  // namespace faas
