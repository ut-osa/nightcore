#include "gateway/message_connection.h"

#include "common/time.h"
#include "common/uv.h"
#include "gateway/server.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

using protocol::Message;
using protocol::Role;
using protocol::HandshakeMessage;
using protocol::HandshakeResponse;

MessageConnection::MessageConnection(Server* server)
    : Connection(Connection::Type::Message, server), io_worker_(nullptr), state_(kCreated),
      role_(Role::INVALID), func_id_(0), client_id_(0),
      log_header_("MessageConnection[Handshaking]: ") {
}

MessageConnection::~MessageConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

uv_stream_t* MessageConnection::InitUVHandle(uv_loop_t* uv_loop) {
    UV_DCHECK_OK(uv_pipe_init(uv_loop, &uv_pipe_handle_, 0));
    return UV_AS_STREAM(&uv_pipe_handle_);
}

void MessageConnection::Start(IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    io_worker_ = io_worker;
    uv_pipe_handle_.data = this;
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_pipe_handle_),
                               &MessageConnection::BufferAllocCallback,
                               &MessageConnection::ReadHandshakeCallback));
    state_ = kHandshake;
}

void MessageConnection::ScheduleClose() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kHandshake || state_ == kRunning);
    uv_close(UV_AS_HANDLE(&uv_pipe_handle_), &MessageConnection::CloseCallback);
    state_ = kClosing;
}

void MessageConnection::SendPendingMessages() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    if (state_ != kRunning) {
        HLOG(WARNING) << "MessageConnection is closing or has closed, will not send pending messages";
        return;
    }
    size_t write_size = 0;
    {
        absl::MutexLock lk(&write_message_mu_);
        write_size = pending_messages_.size() * sizeof(Message);
        if (write_size > 0) {
            write_message_buffer_.Reset();
            write_message_buffer_.AppendData(
                reinterpret_cast<char*>(pending_messages_.data()),
                write_size);
            pending_messages_.clear();
        }
    }
    if (write_size == 0) {
        return;
    }
    io_worker_->write_size_stat()->AddSample(write_size);
    char* ptr = write_message_buffer_.data();
    while (write_size > 0) {
        uv_buf_t buf;
        io_worker_->NewWriteBuffer(&buf);
        size_t copy_size = std::min(buf.len, write_size);
        memcpy(buf.base, ptr, copy_size);
        buf.len = copy_size;
        uv_write_t* write_req = io_worker_->NewWriteRequest();
        write_req->data = buf.base;
        UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(&uv_pipe_handle_),
                              &buf, 1, &MessageConnection::WriteMessageCallback));
        write_size -= copy_size;
        ptr += copy_size;
    }
}

void MessageConnection::RecvHandshakeMessage() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(&uv_pipe_handle_)));
    HandshakeMessage* message = reinterpret_cast<HandshakeMessage*>(
        message_buffer_.data());
    server_->OnNewHandshake(this, *message, &handshake_response_);
    role_ = static_cast<Role>(message->role);
    func_id_ = message->func_id;
    client_id_ = handshake_response_.client_id;
    if (role_ == Role::WATCHDOG) {
        log_header_ = absl::StrFormat("WatchdogConnection[%d]: ", static_cast<int>(func_id_));
    } else if (role_ == Role::FUNC_WORKER) {
        log_header_ = absl::StrFormat("FuncWorkerConnection[%d-%d]: ",
                                      static_cast<int>(func_id_), static_cast<int>(client_id_));
    }
    uv_buf_t buf = {
        .base = reinterpret_cast<char*>(&handshake_response_),
        .len = sizeof(HandshakeResponse)
    };
    UV_DCHECK_OK(uv_write(io_worker_->NewWriteRequest(), UV_AS_STREAM(&uv_pipe_handle_),
                          &buf, 1, &MessageConnection::WriteHandshakeResponseCallback));
    state_ = kRunning;
}

void MessageConnection::WriteMessage(const Message& message) {
    {
        absl::MutexLock lk(&write_message_mu_);
        pending_messages_.push_back(message);
    }
    io_worker_->ScheduleFunction(
        this, absl::bind_front(&MessageConnection::SendPendingMessages, this));
}

UV_ALLOC_CB_FOR_CLASS(MessageConnection, BufferAlloc) {
    io_worker_->NewReadBuffer(suggested_size, buf);
}

UV_READ_CB_FOR_CLASS(MessageConnection, ReadHandshake) {
    auto reclaim_worker_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            io_worker_->ReturnReadBuffer(buf);
        }
    });
    if (nread < 0) {
        HLOG(ERROR) << "Read error on handshake, will close this connection: "
                    << uv_strerror(nread);
        ScheduleClose();
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    message_buffer_.AppendData(buf->base, nread);
    if (message_buffer_.length() > sizeof(HandshakeMessage)) {
        HLOG(ERROR) << "Invalid handshake, will close this connection";
        ScheduleClose();
    } else if (message_buffer_.length() == sizeof(HandshakeMessage)) {
        RecvHandshakeMessage();
    }
}

UV_WRITE_CB_FOR_CLASS(MessageConnection, WriteHandshakeResponse) {
    if (status != 0) {
        HLOG(ERROR) << "Failed to write handshake response, will close this connection: "
                    << uv_strerror(status);
        ScheduleClose();
        return;
    }
    HLOG(INFO) << "Handshake done";
    message_buffer_.Reset();
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_pipe_handle_),
                               &MessageConnection::BufferAllocCallback,
                               &MessageConnection::ReadMessageCallback));
}

UV_READ_CB_FOR_CLASS(MessageConnection, ReadMessage) {
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
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    io_worker_->bytes_per_read_stat()->AddSample(nread);
    utils::ReadMessages<Message>(
        &message_buffer_, buf->base, nread,
        [this] (Message* message) {
            server_->OnRecvMessage(this, *message);
        });
}

UV_WRITE_CB_FOR_CLASS(MessageConnection, WriteMessage) {
    auto reclaim_worker_resource = gsl::finally([this, req] {
        io_worker_->ReturnWriteBuffer(reinterpret_cast<char*>(req->data));
        io_worker_->ReturnWriteRequest(req);
    });
    if (status != 0) {
        HLOG(ERROR) << "Failed to write response, will close this connection: "
                    << uv_strerror(status);
        ScheduleClose();
    }
}

UV_CLOSE_CB_FOR_CLASS(MessageConnection, Close) {
    DCHECK(state_ == kClosing);
    state_ = kClosed;
    io_worker_->OnConnectionClose(this);
}

}  // namespace gateway
}  // namespace faas
