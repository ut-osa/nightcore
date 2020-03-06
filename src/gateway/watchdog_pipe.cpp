#include "gateway/watchdog_pipe.h"

#include "utils/uv_utils.h"
#include "gateway/server.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

using protocol::Status;
using protocol::kMaxFuncNameLength;
using protocol::WatchdogHandshakeMessage;
using protocol::WatchdogHandshakeResponse;

WatchdogPipe::WatchdogPipe(Server* server)
    : server_(server), state_(kCreated),
      log_header_("WatchdogPipe[Handshaking]: ") {
}

WatchdogPipe::~WatchdogPipe() {
    CHECK(state_ == kCreated || state_ == kClosed);
}

void WatchdogPipe::Start(utils::BufferPool* buffer_pool) {
    CHECK(state_ == kCreated);
    CHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    uv_pipe_handle_.data = this;
    write_req_.data = this;
    buffer_pool_ = buffer_pool;
    UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                              &WatchdogPipe::BufferAllocCallback,
                              &WatchdogPipe::ReadHandshakeCallback));
    state_ = kHandshake;
}

void WatchdogPipe::ScheduleClose() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    if (state_ == kClosing) {
        HLOG(INFO) << "Already scheduled for closing";
        return;
    }
    CHECK(state_ == kHandshake || state_ == kRunning);
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_pipe_handle_),
             &WatchdogPipe::CloseCallback);
    state_ = kClosing;
}

void WatchdogPipe::RecvHandshakeMessage() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    UV_CHECK_OK(uv_read_stop(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_)));
    WatchdogHandshakeMessage* message = reinterpret_cast<WatchdogHandshakeMessage*>(
        message_buffer_.data());
    size_t func_name_length = strnlen(
        message->func_name, kMaxFuncNameLength + 1);
    CHECK_LE(func_name_length, kMaxFuncNameLength);
    func_name_.assign(message->func_name, func_name_length);
    log_header_ = absl::StrFormat("WatchdogPipe[%s]: ", func_name_);
    handshake_response_.status = static_cast<uint16_t>(Status::OK);
    uv_buf_t buf = {
        .base = reinterpret_cast<char*>(&handshake_response_),
        .len = sizeof(WatchdogHandshakeResponse)
    };
    UV_CHECK_OK(uv_write(&write_req_, reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                         &buf, 1, &WatchdogPipe::WriteHandshakeResponseCallback));
}

UV_ALLOC_CB_FOR_CLASS(WatchdogPipe, BufferAlloc) {
    buffer_pool_->Get(buf);
}

UV_READ_CB_FOR_CLASS(WatchdogPipe, ReadHandshake) {
    if (nread < 0) {
        HLOG(WARNING) << "Read error on handshake, will close this watchdog pipe: "
                      << uv_strerror(nread);
        ScheduleClose();
    } else if (nread > 0) {
        message_buffer_.AppendData(buf->base, nread);
        CHECK_LE(message_buffer_.length(), sizeof(WatchdogHandshakeMessage));
        if (message_buffer_.length() == sizeof(WatchdogHandshakeMessage)) {
            RecvHandshakeMessage();
        }
    }
    if (buf->base != 0) {
        buffer_pool_->Return(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(WatchdogPipe, WriteHandshakeResponse) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to write handshake response, will close this watchdog pipe: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    HLOG(INFO) << "Handshake done";
    UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                              &WatchdogPipe::BufferAllocCallback,
                              &WatchdogPipe::ReadMessageCallback));
    state_ = kRunning;
}

UV_READ_CB_FOR_CLASS(WatchdogPipe, ReadMessage) {
    if (nread < 0) {
        HLOG(WARNING) << "Read error, will close this watchdog pipe: "
                      << uv_strerror(nread);
        ScheduleClose();
    } else if (nread > 0) {
        // TODO
    }
    if (buf->base != 0) {
        buffer_pool_->Return(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(WatchdogPipe, WriteResponse) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to write response, will close this watchdog pipe: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    //TODO
}

UV_CLOSE_CB_FOR_CLASS(WatchdogPipe, Close) {
    state_ = kClosed;
    server_->OnWatchdogPipeClose(this);
}

}  // namespace gateway
}  // namespace faas
