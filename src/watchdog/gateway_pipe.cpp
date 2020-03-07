#include "watchdog/gateway_pipe.h"

#include "watchdog/watchdog.h"

#define HLOG(l) LOG(l) << "GatewayPipe: "
#define HVLOG(l) VLOG(l) << "GatewayPipe: "

namespace faas {
namespace watchdog {

using protocol::Message;
using protocol::HandshakeMessage;
using protocol::HandshakeResponse;

GatewayPipe::GatewayPipe(Watchdog* watchdog)
    : watchdog_(watchdog), state_(kCreated) {}

GatewayPipe::~GatewayPipe() {
    CHECK(state_ == kCreated || state_ == kClosed);
}

void GatewayPipe::Start(absl::string_view ipc_path, utils::BufferPool* buffer_pool,
                        const HandshakeMessage& handshake_message) {
    CHECK(state_ == kCreated);
    buffer_pool_ = buffer_pool;
    handshake_message_ = handshake_message;
    uv_pipe_handle_.data = this;
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
    UV_CHECK_OK(uv_read_stop(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_)));
    HandshakeResponse* response = reinterpret_cast<HandshakeResponse*>(
        message_buffer_.data());
    if (watchdog_->OnRecvHandshakeResponse(*response)) {
        HLOG(INFO) << "Handshake done";
        message_buffer_.Reset();
        UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                                &GatewayPipe::BufferAllocCallback,
                                &GatewayPipe::ReadMessageCallback));
        state_ = kRunning;
    }
}

void GatewayPipe::WriteWMessage(const Message& message) {
    CHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    uv_buf_t buf;
    buffer_pool_->Get(&buf);
    CHECK_LE(sizeof(Message), buf.len);
    memcpy(buf.base, &message, sizeof(Message));
    buf.len = sizeof(Message);
    uv_write_t* write_req = write_req_pool_.Get();
    write_req->data = buf.base;
    UV_CHECK_OK(uv_write(write_req, reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                         &buf, 1, &GatewayPipe::WriteMessageCallback));
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
        .len = sizeof(HandshakeMessage)
    };
    UV_CHECK_OK(uv_write(write_req_pool_.Get(), reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
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
        CHECK_LE(message_buffer_.length(), sizeof(HandshakeResponse));
        if (message_buffer_.length() == sizeof(HandshakeResponse)) {
            RecvHandshakeResponse();
        }
    }
    if (buf->base != 0) {
        buffer_pool_->Return(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(GatewayPipe, WriteHandshake) {
    write_req_pool_.Return(req);
    if (status != 0) {
        HLOG(WARNING) << "Failed to write handshake message, will close the pipe: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                              &GatewayPipe::BufferAllocCallback,
                              &GatewayPipe::ReadHandshakeResponseCallback));
}

UV_READ_CB_FOR_CLASS(GatewayPipe, ReadMessage) {
    if (nread < 0) {
        HLOG(WARNING) << "Read error, will close the pipe: "
                      << uv_strerror(nread);
        ScheduleClose();
    } else if (nread > 0) {
        utils::ReadMessages<Message>(
            &message_buffer_, buf->base, nread,
            [this] (Message* message) {
                watchdog_->OnRecvMessage(*message);
            });
    }
    if (buf->base != 0) {
        buffer_pool_->Return(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(GatewayPipe, WriteMessage) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to write response, will close the pipe: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    buffer_pool_->Return(reinterpret_cast<char*>(req->data));
    write_req_pool_.Return(req);
}

UV_CLOSE_CB_FOR_CLASS(GatewayPipe, Close) {
    state_ = kClosed;
    watchdog_->OnGatewayPipeClose();
}

}  // namespace watchdog
}  // namespace faas
