#include "gateway/engine_connection.h"

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
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_tcp_handle_),
                               &EngineConnection::BufferAllocCallback,
                               &EngineConnection::RecvDataCallback));
    state_ = kRunning;
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
    // TODO
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
    // TODO
}

UV_WRITE_CB_FOR_CLASS(EngineConnection, DataWritten) {
    // TODO
}

UV_CLOSE_CB_FOR_CLASS(EngineConnection, Close) {
    DCHECK(state_ == kClosing);
    state_ = kClosed;
    io_worker_->OnConnectionClose(this);
}

}  // namespace gateway
}  // namespace faas
