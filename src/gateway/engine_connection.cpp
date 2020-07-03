#include "gateway/engine_connection.h"

#include "gateway/server.h"

#include <absl/flags/flag.h>

ABSL_FLAG(bool, engine_conn_enable_nodelay, true,
          "Enable TCP_NODELAY for connections to engines");
ABSL_FLAG(bool, engine_conn_enable_keepalive, true,
          "Enable TCP keep-alive for connections to engines");

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

using protocol::GatewayMessage;

EngineConnection::EngineConnection(Server* server, uint16_t node_id, uint16_t conn_id,
                                   std::span<const char> initial_data)
    : server::ConnectionBase(type_id(node_id)),
      server_(server), node_id_(node_id), conn_id_(conn_id), state_(kCreated),
      log_header_(fmt::format("EngineConnection[{}-{}]: ", node_id, conn_id)) {
    read_buffer_.AppendData(initial_data);
}

EngineConnection::~EngineConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

uv_stream_t* EngineConnection::InitUVHandle(uv_loop_t* uv_loop) {
    UV_DCHECK_OK(uv_tcp_init(uv_loop, &uv_tcp_handle_));
    return UV_AS_STREAM(&uv_tcp_handle_);
}

void EngineConnection::Start(server::IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    io_worker_ = io_worker;
    uv_tcp_handle_.data = this;
    if (absl::GetFlag(FLAGS_engine_conn_enable_nodelay)) {
        UV_DCHECK_OK(uv_tcp_nodelay(&uv_tcp_handle_, 1));
    }
    if (absl::GetFlag(FLAGS_engine_conn_enable_keepalive)) {
        UV_DCHECK_OK(uv_tcp_keepalive(&uv_tcp_handle_, 1, 1));
    }
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_tcp_handle_),
                               &EngineConnection::BufferAllocCallback,
                               &EngineConnection::RecvDataCallback));
    state_ = kRunning;
    ProcessGatewayMessages();
}

void EngineConnection::ScheduleClose() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    uv_close(UV_AS_HANDLE(&uv_tcp_handle_), &EngineConnection::CloseCallback);
    state_ = kClosing;
}

void EngineConnection::SendMessage(const GatewayMessage& message, std::span<const char> payload) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "EngineConnection is closing or has closed, will not send this message";
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
                              &buf, 1, &EngineConnection::DataSentCallback));
        pos += write_size;
    }
}

void EngineConnection::ProcessGatewayMessages() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_tcp_handle_.loop);
    while (read_buffer_.length() >= sizeof(GatewayMessage)) {
        GatewayMessage* message = reinterpret_cast<GatewayMessage*>(read_buffer_.data());
        size_t full_size = sizeof(GatewayMessage) + std::max<size_t>(0, message->payload_size);
        if (read_buffer_.length() >= full_size) {
            std::span<const char> payload(read_buffer_.data() + sizeof(GatewayMessage),
                                          full_size - sizeof(GatewayMessage));
            server_->OnRecvEngineMessage(this, *message, payload);
            read_buffer_.ConsumeFront(full_size);
        } else {
            break;
        }
    }
}

UV_ALLOC_CB_FOR_CLASS(EngineConnection, BufferAlloc) {
    io_worker_->NewReadBuffer(suggested_size, buf);
}

UV_READ_CB_FOR_CLASS(EngineConnection, RecvData) {
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

UV_WRITE_CB_FOR_CLASS(EngineConnection, DataSent) {
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

UV_CLOSE_CB_FOR_CLASS(EngineConnection, Close) {
    DCHECK(state_ == kClosing);
    state_ = kClosed;
    io_worker_->OnConnectionClose(this);
}

}  // namespace gateway
}  // namespace faas
