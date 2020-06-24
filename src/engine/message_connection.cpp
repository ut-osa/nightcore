#include "engine/message_connection.h"

#include "common/time.h"
#include "common/uv.h"
#include "ipc/base.h"
#include "ipc/fifo.h"
#include "engine/engine.h"

#include <absl/flags/flag.h>

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

ABSL_FLAG(bool, func_worker_pipe_direct_write, false, "");

namespace faas {
namespace engine {

using protocol::Message;
using protocol::IsLauncherHandshakeMessage;
using protocol::IsFuncWorkerHandshakeMessage;

MessageConnection::MessageConnection(Engine* engine)
    : server::ConnectionBase(kTypeId), engine_(engine), io_worker_(nullptr),
      state_(kCreated), func_id_(0), client_id_(0), handshake_done_(false),
      pipe_for_write_fd_(-1),
      log_header_("MessageConnection[Handshaking]: ") {
}

MessageConnection::~MessageConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

uv_stream_t* MessageConnection::InitUVHandle(uv_loop_t* uv_loop) {
    UV_DCHECK_OK(uv_pipe_init(uv_loop, &uv_pipe_handle_, 0));
    handle_scope_.Init(uv_loop, absl::bind_front(&MessageConnection::OnAllHandlesClosed, this));
    handle_scope_.AddHandle(&uv_pipe_handle_);
    return UV_AS_STREAM(&uv_pipe_handle_);
}

void MessageConnection::Start(server::IOWorker* io_worker) {
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
    handle_scope_.CloseHandle(&uv_pipe_handle_);
    if (client_id_ > 0) {
        handle_scope_.CloseHandle(&uv_in_fifo_handle_);
        handle_scope_.CloseHandle(&uv_out_fifo_handle_);
    }
    state_ = kClosing;
}

void MessageConnection::SendPendingMessages() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
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
        UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(pipe_for_write_message_),
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
    DCHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(&uv_pipe_handle_)));
    Message* message = reinterpret_cast<Message*>(message_buffer_.data());
    func_id_ = message->func_id;
    if (IsLauncherHandshakeMessage(*message)) {
        client_id_ = 0;
        log_header_ = absl::StrFormat("LauncherConnection[%d]: ", func_id_);
    } else if (IsFuncWorkerHandshakeMessage(*message)) {
        client_id_ = message->client_id;
        log_header_ = absl::StrFormat("FuncWorkerConnection[%d-%d]: ", func_id_, client_id_);
    } else {
        HLOG(FATAL) << "Unknown handshake message type";
    }
    std::span<const char> payload;
    if (!engine_->OnNewHandshake(this, *message, &handshake_response_, &payload)) {
        ScheduleClose();
        return;
    }
    if (IsLauncherHandshakeMessage(*message)) {
        pipe_for_read_message_ = &uv_pipe_handle_;
        pipe_for_write_message_ = &uv_pipe_handle_;
    } else if (IsFuncWorkerHandshakeMessage(*message)) {
        // Open input FIFO
        UV_DCHECK_OK(uv_pipe_init(uv_pipe_handle_.loop, &uv_in_fifo_handle_, 0));
        uv_in_fifo_handle_.data = this;
        int in_fifo_fd = ipc::FifoOpenForWrite(ipc::GetFuncWorkerInputFifoName(client_id_));
        if (in_fifo_fd == -1) {
            HLOG(ERROR) << "FifoOpenForWrite failed";
            ScheduleClose();
            return;
        }
        ipc::FifoUnsetNonblocking(in_fifo_fd);
        UV_DCHECK_OK(uv_pipe_open(&uv_in_fifo_handle_, in_fifo_fd));
        handle_scope_.AddHandle(&uv_in_fifo_handle_);
        // Open output FIFO
        UV_DCHECK_OK(uv_pipe_init(uv_pipe_handle_.loop, &uv_out_fifo_handle_, 0));
        uv_out_fifo_handle_.data = this;
        int out_fifo_fd = ipc::FifoOpenForRead(ipc::GetFuncWorkerOutputFifoName(client_id_));
        if (out_fifo_fd == -1) {
            HLOG(ERROR) << "FifoOpenForRead failed";
            ScheduleClose();
            return;
        }
        UV_DCHECK_OK(uv_pipe_open(&uv_out_fifo_handle_, out_fifo_fd));
        handle_scope_.AddHandle(&uv_out_fifo_handle_);
        // Use FIFOs for sending and receiving messages
        pipe_for_read_message_ = &uv_out_fifo_handle_;
        pipe_for_write_message_ = &uv_in_fifo_handle_;
    } else {
        HLOG(FATAL) << "Unknown handshake message type";
    }
    uv_buf_t bufs[2];
    bufs[0] = {
        .base = reinterpret_cast<char*>(&handshake_response_),
        .len = sizeof(Message)
    };
    bufs[1] = {
        .base = const_cast<char*>(payload.data()),
        .len = payload.size()
    };
    UV_DCHECK_OK(uv_write(io_worker_->NewWriteRequest(), UV_AS_STREAM(&uv_pipe_handle_),
                          bufs, 2, &MessageConnection::WriteHandshakeResponseCallback));
    handshake_done_ = true;
    state_ = kRunning;
    SendPendingMessages();
}

void MessageConnection::WriteMessage(const Message& message) {
    if (!is_launcher_connection() && absl::GetFlag(FLAGS_func_worker_pipe_direct_write)) {
        int fd = pipe_for_write_fd_.load();
        if (fd != -1) {
            ssize_t ret = write(pipe_for_write_fd_, &message, sizeof(Message));
            if (ret > 0) {
                if (gsl::narrow_cast<size_t>(ret) == sizeof(Message)) {
                    // All good
                    return;
                } else {
                    HLOG(FATAL) << "This should not happen given atomic property of pipe";
                }
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
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(pipe_for_read_message_),
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
    } else {
        if (!is_launcher_connection() && absl::GetFlag(FLAGS_func_worker_pipe_direct_write)) {
            int fd = -1;
            UV_DCHECK_OK(uv_fileno(UV_AS_HANDLE(pipe_for_write_message_), &fd));
            pipe_for_write_fd_.store(fd);
        }
    }
}

}  // namespace engine
}  // namespace faas
