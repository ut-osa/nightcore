#include "engine/gateway_connection.h"

#include "engine/engine.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

#include <absl/flags/flag.h>

ABSL_FLAG(bool, gateway_conn_enable_nodelay, true,
          "Enable TCP_NODELAY for connections to gateway");
ABSL_FLAG(bool, gateway_conn_enable_keepalive, true,
          "Enable TCP keep-alive for connections to gateway");

namespace faas {
namespace engine {

using protocol::GatewayMessage;
using protocol::NewEngineHandshakeGatewayMessage;

GatewayConnection::GatewayConnection(Engine* engine, uint16_t conn_id)
    : server::ConnectionBase(kTypeId),
      engine_(engine), conn_id_(conn_id), state_(kCreated),
      log_header_(fmt::format("GatewayConnection[{}]: ", conn_id)) {}

GatewayConnection::~GatewayConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

uv_stream_t* GatewayConnection::InitUVHandle(uv_loop_t* uv_loop) {
    UV_DCHECK_OK(uv_tcp_init(uv_loop, &uv_tcp_handle_));
    return UV_AS_STREAM(&uv_tcp_handle_);
}

void GatewayConnection::Start(server::IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    io_worker_ = io_worker;
    uv_tcp_handle_.data = this;
    if (absl::GetFlag(FLAGS_gateway_conn_enable_nodelay)) {
        UV_DCHECK_OK(uv_tcp_nodelay(&uv_tcp_handle_, 1));
    }
    if (absl::GetFlag(FLAGS_gateway_conn_enable_keepalive)) {
        UV_DCHECK_OK(uv_tcp_keepalive(&uv_tcp_handle_, 1, 1));
    }
    handshake_message_ = NewEngineHandshakeGatewayMessage(engine_->node_id(), conn_id_);
    uv_buf_t buf = {
        .base = reinterpret_cast<char*>(&handshake_message_),
        .len = sizeof(GatewayMessage)
    };
    UV_DCHECK_OK(uv_write(io_worker_->NewWriteRequest(), UV_AS_STREAM(&uv_tcp_handle_),
                          &buf, 1, &GatewayConnection::HandshakeSentCallback));
    state_ = kHandshake;
}

void GatewayConnection::ScheduleClose() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kHandshake || state_ == kRunning);
    uv_close(UV_AS_HANDLE(&uv_tcp_handle_), &GatewayConnection::CloseCallback);
    state_ = kClosing;
}

void GatewayConnection::SendMessage(const GatewayMessage& message, std::span<const char> payload) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "GatewayConnection is closing or has closed, will not send this message";
        return;
    }
    DCHECK_EQ(message.payload_size, gsl::narrow_cast<int32_t>(payload.size()));
    size_t pos = 0;
    while (pos < sizeof(GatewayMessage) + payload.size()) {
        uv_buf_t buf;
        io_worker_->NewWriteBuffer(&buf);
        size_t write_size;
        if (pos == 0) {
            DCHECK(sizeof(GatewayMessage) <= buf.len);
            memcpy(buf.base, &message, sizeof(GatewayMessage));
            size_t copy_size = std::min(buf.len - sizeof(GatewayMessage), payload.size());
            memcpy(buf.base + sizeof(GatewayMessage), payload.data(), copy_size);
            write_size = sizeof(GatewayMessage) + copy_size;
        } else {
            size_t copy_size = std::min(buf.len, payload.size() + sizeof(GatewayMessage) - pos);
            memcpy(buf.base, payload.data() + pos - sizeof(GatewayMessage), copy_size);
            write_size = copy_size;
        }
        DCHECK_LE(write_size, buf.len);
        buf.len = write_size;
        uv_write_t* write_req = io_worker_->NewWriteRequest();
        write_req->data = buf.base;
        UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(&uv_tcp_handle_),
                              &buf, 1, &GatewayConnection::DataSentCallback));
        pos += write_size;
    }
}

void GatewayConnection::ProcessGatewayMessages() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    while (read_buffer_.length() >= sizeof(GatewayMessage)) {
        GatewayMessage* message = reinterpret_cast<GatewayMessage*>(read_buffer_.data());
        size_t full_size = sizeof(GatewayMessage) + std::max<size_t>(0, message->payload_size);
        if (read_buffer_.length() >= full_size) {
            std::span<const char> payload(read_buffer_.data() + sizeof(GatewayMessage),
                                          full_size - sizeof(GatewayMessage));
            engine_->OnRecvGatewayMessage(this, *message, payload);
            read_buffer_.ConsumeFront(full_size);
        } else {
            break;
        }
    }
}

UV_ALLOC_CB_FOR_CLASS(GatewayConnection, BufferAlloc) {
    io_worker_->NewReadBuffer(suggested_size, buf);
}

UV_READ_CB_FOR_CLASS(GatewayConnection, RecvData) {
    auto reclaim_worker_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            io_worker_->ReturnReadBuffer(buf);
        }
    });
    if (nread < 0) {
        if (nread == UV_EOF) {
            HLOG(INFO) << "Connection closed remotely";
        } else {
            HLOG(ERROR) << "Read error, will close this connection: "
                        << uv_strerror(nread);
        }
        ScheduleClose();
        return;
    }
    if (nread == 0) {
        return;
    }
    read_buffer_.AppendData(buf->base, nread);
    ProcessGatewayMessages();
}

UV_WRITE_CB_FOR_CLASS(GatewayConnection, HandshakeSent) {
    if (status != 0) {
        HLOG(ERROR) << "Failed to send handshake, will close this connection: "
                    << uv_strerror(status);
        ScheduleClose();
        return;
    }
    HLOG(INFO) << "Handshake done";
    state_ = kRunning;
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_tcp_handle_),
                               &GatewayConnection::BufferAllocCallback,
                               &GatewayConnection::RecvDataCallback));
}

UV_WRITE_CB_FOR_CLASS(GatewayConnection, DataSent) {
    auto reclaim_worker_resource = gsl::finally([this, req] {
        io_worker_->ReturnWriteBuffer(reinterpret_cast<char*>(req->data));
        io_worker_->ReturnWriteRequest(req);
    });
    if (status != 0) {
        HLOG(ERROR) << "Failed to send data, will close this connection: "
                    << uv_strerror(status);
        ScheduleClose();
    }
}

UV_CLOSE_CB_FOR_CLASS(GatewayConnection, Close) {
    DCHECK(state_ == kClosing);
    state_ = kClosed;
    io_worker_->OnConnectionClose(this);
}

}  // namespace engine
}  // namespace faas
