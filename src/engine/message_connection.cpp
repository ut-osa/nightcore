#include "engine/message_connection.h"

#include "common/time.h"
#include "common/uv.h"
#include "ipc/base.h"
#include "ipc/fifo.h"
#include "engine/engine.h"

#include <absl/flags/flag.h>

ABSL_FLAG(bool, func_worker_pipe_direct_write, false, "");

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace engine {

using protocol::Message;
using protocol::IsLauncherHandshakeMessage;
using protocol::IsFuncWorkerHandshakeMessage;

MessageConnection::MessageConnection(Engine* engine)
    : server::ConnectionBase(kTypeId), engine_(engine), io_worker_(nullptr),
      state_(kCreated), func_id_(0), client_id_(0), handshake_done_(false),
      uv_handle_(nullptr), pipe_for_write_fd_(-1),
      log_header_("MessageConnection[Handshaking]: ") {
}

MessageConnection::~MessageConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
    if (uv_handle_ != nullptr) {
        free(uv_handle_);
    }
}

uv_stream_t* MessageConnection::InitUVHandle(uv_loop_t* uv_loop) {
    if (engine_->engine_tcp_port() == -1) {
        uv_handle_ = UV_AS_STREAM(malloc(sizeof(uv_pipe_t)));
        UV_DCHECK_OK(uv_pipe_init(uv_loop, reinterpret_cast<uv_pipe_t*>(uv_handle_), 0));
    } else {
        uv_handle_ = UV_AS_STREAM(malloc(sizeof(uv_tcp_t)));
        UV_DCHECK_OK(uv_tcp_init(uv_loop, reinterpret_cast<uv_tcp_t*>(uv_handle_)));
    }
    handle_scope_.Init(uv_loop, absl::bind_front(&MessageConnection::OnAllHandlesClosed, this));
    handle_scope_.AddHandle(uv_handle_);
    return uv_handle_;
}

void MessageConnection::Start(server::IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK_IN_EVENT_LOOP_THREAD(uv_handle_->loop);
    io_worker_ = io_worker;
    uv_handle_->data = this;
    UV_DCHECK_OK(uv_read_start(uv_handle_,
                               &MessageConnection::BufferAllocCallback,
                               &MessageConnection::ReadHandshakeCallback));
    state_ = kHandshake;
}

void MessageConnection::ScheduleClose() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_handle_->loop);
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kHandshake || state_ == kRunning);
    handle_scope_.CloseHandle(uv_handle_);
    if (client_id_ > 0 && !engine_->func_worker_use_engine_socket()) {
        handle_scope_.CloseHandle(&in_fifo_handle_);
        handle_scope_.CloseHandle(&out_fifo_handle_);
    }
    state_ = kClosing;
}

void MessageConnection::SendPendingMessages() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_handle_->loop);
    if (state_ == kHandshake) {
        return;
    }
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
    char* ptr = write_message_buffer_.data();
    while (write_size > 0) {
        uv_buf_t buf;
        io_worker_->NewWriteBuffer(&buf);
        size_t copy_size = std::min(buf.len, write_size);
        memcpy(buf.base, ptr, copy_size);
        buf.len = copy_size;
        uv_write_t* write_req = io_worker_->NewWriteRequest();
        write_req->data = buf.base;
        UV_DCHECK_OK(uv_write(write_req, handle_for_write_message_,
                              &buf, 1, &MessageConnection::WriteMessageCallback));
        write_size -= copy_size;
        ptr += copy_size;
    }
}

void MessageConnection::OnAllHandlesClosed() {
    DCHECK(state_ == kClosing);
    state_ = kClosed;
    io_worker_->OnConnectionClose(this);
}

void MessageConnection::RecvHandshakeMessage() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_handle_->loop);
    UV_DCHECK_OK(uv_read_stop(uv_handle_));
    Message* message = reinterpret_cast<Message*>(message_buffer_.data());
    func_id_ = message->func_id;
    if (IsLauncherHandshakeMessage(*message)) {
        client_id_ = 0;
        log_header_ = fmt::format("LauncherConnection[{}]: ", func_id_);
    } else if (IsFuncWorkerHandshakeMessage(*message)) {
        client_id_ = message->client_id;
        log_header_ = fmt::format("FuncWorkerConnection[{}-{}]: ", func_id_, client_id_);
    } else {
        HLOG(FATAL) << "Unknown handshake message type";
    }
    std::span<const char> payload;
    if (!engine_->OnNewHandshake(this, *message, &handshake_response_, &payload)) {
        ScheduleClose();
        return;
    }
    if (IsLauncherHandshakeMessage(*message)) {
        handle_for_read_message_ = uv_handle_;
        handle_for_write_message_ = uv_handle_;
    } else if (IsFuncWorkerHandshakeMessage(*message)) {
        if (engine_->func_worker_use_engine_socket()) {
            handle_for_read_message_ = uv_handle_;
            handle_for_write_message_ = uv_handle_;
        } else {
            // Open input FIFO
            UV_DCHECK_OK(uv_pipe_init(uv_handle_->loop, &in_fifo_handle_, 0));
            in_fifo_handle_.data = this;
            int in_fifo_fd = ipc::FifoOpenForWrite(ipc::GetFuncWorkerInputFifoName(client_id_));
            if (in_fifo_fd == -1) {
                HLOG(ERROR) << "FifoOpenForWrite failed";
                ScheduleClose();
                return;
            }
            ipc::FifoUnsetNonblocking(in_fifo_fd);
            UV_DCHECK_OK(uv_pipe_open(&in_fifo_handle_, in_fifo_fd));
            handle_scope_.AddHandle(&in_fifo_handle_);
            // Open output FIFO
            UV_DCHECK_OK(uv_pipe_init(uv_handle_->loop, &out_fifo_handle_, 0));
            out_fifo_handle_.data = this;
            int out_fifo_fd = ipc::FifoOpenForRead(ipc::GetFuncWorkerOutputFifoName(client_id_));
            if (out_fifo_fd == -1) {
                HLOG(ERROR) << "FifoOpenForRead failed";
                ScheduleClose();
                return;
            }
            UV_DCHECK_OK(uv_pipe_open(&out_fifo_handle_, out_fifo_fd));
            handle_scope_.AddHandle(&out_fifo_handle_);
            // Use FIFOs for sending and receiving messages
            handle_for_read_message_ = UV_AS_STREAM(&out_fifo_handle_);
            handle_for_write_message_ = UV_AS_STREAM(&in_fifo_handle_);
            pipe_for_write_fd_.store(in_fifo_fd);
        }
    } else {
        HLOG(FATAL) << "Unknown handshake message type";
    }
    if (payload.size() > 0) {
        uv_buf_t bufs[2];
        bufs[0] = {
            .base = reinterpret_cast<char*>(&handshake_response_),
            .len = sizeof(Message)
        };
        bufs[1] = {
            .base = const_cast<char*>(payload.data()),
            .len = payload.size()
        };
        UV_DCHECK_OK(uv_write(io_worker_->NewWriteRequest(), uv_handle_,
                              bufs, 2, &MessageConnection::WriteHandshakeResponseCallback));
    } else {
        uv_buf_t buf = {
            .base = reinterpret_cast<char*>(&handshake_response_),
            .len = sizeof(Message)
        };
        UV_DCHECK_OK(uv_write(io_worker_->NewWriteRequest(), uv_handle_,
                              &buf, 1, &MessageConnection::WriteHandshakeResponseCallback));
    }
    handshake_done_ = true;
    state_ = kRunning;
    SendPendingMessages();
}

void MessageConnection::WriteMessage(const Message& message) {
    if (is_func_worker_connection()
          && absl::GetFlag(FLAGS_func_worker_pipe_direct_write)
          && !engine_->func_worker_use_engine_socket()) {
        int fd = pipe_for_write_fd_.load();
        if (fd != -1) {
            ssize_t ret = write(fd, &message, sizeof(Message));
            if (ret > 0) {
                if (gsl::narrow_cast<size_t>(ret) == sizeof(Message)) {
                    // All good
                    return;
                } else {
                    HLOG(FATAL) << "This should not happen given atomic property of pipe";
                }
            } else if (ret < 0) {
                PLOG(WARNING) << "Pipe write failed";
            }
        }
        HLOG(INFO) << "Fallback to original WriteMessage";
    }
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
    if (message_buffer_.length() > sizeof(Message)) {
        HLOG(ERROR) << "Invalid handshake, will close this connection";
        ScheduleClose();
    } else if (message_buffer_.length() == sizeof(Message)) {
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
    UV_DCHECK_OK(uv_read_start(handle_for_read_message_,
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
        return;
    }
    utils::ReadMessages<Message>(
        &message_buffer_, buf->base, nread,
        [this] (Message* message) {
            engine_->OnRecvMessage(this, *message);
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

}  // namespace engine
}  // namespace faas
