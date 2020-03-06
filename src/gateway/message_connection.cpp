#include "gateway/message_connection.h"

#include "utils/uv_utils.h"
#include "gateway/server.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

using protocol::HandshakeMessage;
using protocol::HandshakeResponse;

MessageConnection::MessageConnection(Server* server)
    : Connection(Connection::Type::Message, server), io_worker_(nullptr), state_(kCreated),
      log_header_("MessageConnection[Handshaking]: ") {
}

MessageConnection::~MessageConnection() {
    CHECK(state_ == kCreated || state_ == kClosed);
}

uv_stream_t* MessageConnection::InitUVHandle(uv_loop_t* uv_loop) {
    UV_CHECK_OK(uv_pipe_init(uv_loop, &uv_pipe_handle_, 0));
    return reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_);
}

void MessageConnection::Start(IOWorker* io_worker) {
    CHECK(state_ == kCreated);
    CHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    uv_pipe_handle_.data = this;
    write_req_.data = this;
    io_worker_ = io_worker;
    UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                              &MessageConnection::BufferAllocCallback,
                              &MessageConnection::ReadHandshakeCallback));
    state_ = kHandshake;
}

void MessageConnection::ScheduleClose() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    if (state_ == kClosing) {
        HLOG(INFO) << "Already scheduled for closing";
        return;
    }
    CHECK(state_ == kHandshake || state_ == kRunning);
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_pipe_handle_),
             &MessageConnection::CloseCallback);
    state_ = kClosing;
}

void MessageConnection::RecvHandshakeMessage() {
    CHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    UV_CHECK_OK(uv_read_stop(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_)));
    HandshakeMessage* message = reinterpret_cast<HandshakeMessage*>(
        message_buffer_.data());
    server_->OnNewHandshake(this, *message, &handshake_response_);
    uv_buf_t buf = {
        .base = reinterpret_cast<char*>(&handshake_response_),
        .len = sizeof(HandshakeResponse)
    };
    UV_CHECK_OK(uv_write(&write_req_, reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                         &buf, 1, &MessageConnection::WriteHandshakeResponseCallback));
}

UV_ALLOC_CB_FOR_CLASS(MessageConnection, BufferAlloc) {
    io_worker_->NewReadBuffer(suggested_size, buf);
}

UV_READ_CB_FOR_CLASS(MessageConnection, ReadHandshake) {
    if (nread < 0) {
        HLOG(WARNING) << "Read error on handshake, will close this connection: "
                      << uv_strerror(nread);
        ScheduleClose();
    } else if (nread > 0) {
        message_buffer_.AppendData(buf->base, nread);
        CHECK_LE(message_buffer_.length(), sizeof(HandshakeMessage));
        if (message_buffer_.length() == sizeof(HandshakeMessage)) {
            RecvHandshakeMessage();
        }
    }
    if (buf->base != 0) {
        io_worker_->ReturnReadBuffer(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(MessageConnection, WriteHandshakeResponse) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to write handshake response, will close this connection: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    HLOG(INFO) << "Handshake done";
    UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&uv_pipe_handle_),
                              &MessageConnection::BufferAllocCallback,
                              &MessageConnection::ReadMessageCallback));
    state_ = kRunning;
}

UV_READ_CB_FOR_CLASS(MessageConnection, ReadMessage) {
    if (nread < 0) {
        HLOG(WARNING) << "Read error, will close this connection: "
                      << uv_strerror(nread);
        ScheduleClose();
    } else if (nread > 0) {
        // TODO
    }
    if (buf->base != 0) {
        io_worker_->ReturnReadBuffer(buf);
    }
}

UV_WRITE_CB_FOR_CLASS(MessageConnection, WriteResponse) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to write response, will close this connection: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    //TODO
}

UV_CLOSE_CB_FOR_CLASS(MessageConnection, Close) {
    state_ = kClosed;
    io_worker_->OnConnectionClose(this);
}

}  // namespace gateway
}  // namespace faas
