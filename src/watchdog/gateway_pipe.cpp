#include "watchdog/gateway_pipe.h"

#include "watchdog/watchdog.h"

#define HLOG(l) LOG(l) << "GatewayPipe: "
#define HVLOG(l) VLOG(l) << "GatewayPipe: "

namespace faas {
namespace watchdog {

using protocol::OK_STATUS;
using protocol::kMaxFunctionNameLength;
using protocol::WatchdogHandshakeMessage;
using protocol::WatchdogHandshakeResponse;

GatewayPipe::GatewayPipe(Watchdog* watchdog)
    : watchdog_(watchdog), state_(kCreated) {}

GatewayPipe::~GatewayPipe() {
    CHECK(state_ == kCreated || state_ == kClosed);
}

void GatewayPipe::Start(absl::string_view ipc_path, absl::string_view function_name,
                        utils::BufferPool* buffer_pool) {
    CHECK(state_ == kCreated);
    buffer_pool_ = buffer_pool;
    memset(&handshake_message_, 0, sizeof(WatchdogHandshakeMessage));
    CHECK_LE(function_name.length(), kMaxFunctionNameLength);
    memcpy(handshake_message_.function_name,
           function_name.data(), function_name.length());
    uv_pipe_handle_.data = this;
    connect_req_.data = this;
    write_req_.data = this;
    uv_pipe_connect(&connect_req_, &uv_pipe_handle_,
                    std::string(ipc_path).c_str(),
                    &GatewayPipe::ConnectCallback);
    state_ = kHandshake;
}

void GatewayPipe::ScheduleClose() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    if (state_ == kClosing) {
        HLOG(INFO) << "Already scheduled for closing";
        return;
    }
    CHECK(state_ == kHandshake || state_ == kRunning);
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_pipe_handle_),
             &GatewayPipe::CloseCallback);
    state_ = kClosing;
}

void GatewayPipe::RecvHandshakeResponse() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    LIBUV_CHECK_OK(uv_read_stop(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_)));
    WatchdogHandshakeResponse* message = reinterpret_cast<WatchdogHandshakeResponse*>(
        message_buffer_.data());
    if (message->status != OK_STATUS) {
        HLOG(WARNING) << "Handshake failed, will close the pipe";
        ScheduleClose();
        return;
    }
    HLOG(INFO) << "Handshake done";
    LIBUV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                                 &GatewayPipe::BufferAllocCallback,
                                 &GatewayPipe::ReadMessageCallback));
    state_ = kRunning;
}

UV_CONNECT_CB_FOR_CLASS(GatewayPipe, Connect) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to connect to gateway, will close the pipe: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    HLOG(INFO) << "Connected to gateway, start writing handshake message";
    uv_buf_t buf = {
        .base = reinterpret_cast<char*>(&handshake_message_),
        .len = sizeof(WatchdogHandshakeMessage)
    };
    LIBUV_CHECK_OK(uv_write(&write_req_, reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                            &buf, 1, &GatewayPipe::WriteHandshakeCallback));
}

UV_ALLOC_CB_FOR_CLASS(GatewayPipe, BufferAlloc) {
    buffer_pool_->Get(buf);
}

UV_READ_CB_FOR_CLASS(GatewayPipe, ReadHandshakeResponse) {
    if (nread < 0) {
        HLOG(WARNING) << "Read error on handshake, will close the pipe: "
                      << uv_strerror(nread);
        ScheduleClose();
    } else if (nread > 0) {
        message_buffer_.AppendData(buf->base, nread);
        CHECK_LE(message_buffer_.length(), sizeof(WatchdogHandshakeResponse));
        if (message_buffer_.length() == sizeof(WatchdogHandshakeResponse)) {
            RecvHandshakeResponse();
        }
    }
    if (buf->base != 0) {
        buffer_pool_->Return(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(GatewayPipe, WriteHandshake) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to write handshake message, will close the pipe: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    LIBUV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                                 &GatewayPipe::BufferAllocCallback,
                                 &GatewayPipe::ReadHandshakeResponseCallback));
}

UV_READ_CB_FOR_CLASS(GatewayPipe, ReadMessage) {
    if (nread < 0) {
        HLOG(WARNING) << "Read error, will close the pipe: "
                      << uv_strerror(nread);
        ScheduleClose();
    } else if (nread > 0) {
        // TODO
    }
    if (buf->base != 0) {
        buffer_pool_->Return(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(GatewayPipe, WriteResponse) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to write response, will close the pipe: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    //TODO
}

UV_CLOSE_CB_FOR_CLASS(GatewayPipe, Close) {
    state_ = kClosed;
    watchdog_->OnGatewayPipeClose();
}

}  // namespace watchdog
}  // namespace faas
