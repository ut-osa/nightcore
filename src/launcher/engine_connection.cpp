#include "launcher/engine_connection.h"

#include "launcher/launcher.h"

#define HLOG(l) LOG(l) << "EngineConnection: "
#define HVLOG(l) VLOG(l) << "EngineConnection: "

namespace faas {
namespace launcher {

using protocol::Message;

EngineConnection::EngineConnection(Launcher* launcher)
    : launcher_(launcher), state_(kCreated) {}

EngineConnection::~EngineConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void EngineConnection::Start(std::string_view ipc_path, const Message& handshake_message) {
    DCHECK(state_ == kCreated);
    handshake_message_ = handshake_message;
    uv_pipe_handle_.data = this;
    uv_pipe_connect(&connect_req_, &uv_pipe_handle_,
                    std::string(ipc_path).c_str(),
                    &EngineConnection::ConnectCallback);
    state_ = kHandshake;
}

void EngineConnection::ScheduleClose() {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    if (state_ == kHandshake || state_ == kRunning) {
        uv_close(UV_AS_HANDLE(&uv_pipe_handle_), &EngineConnection::CloseCallback);
        state_ = kClosing;
    }
}

void EngineConnection::RecvHandshakeResponse() {
    UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(&uv_pipe_handle_)));
    Message* response = reinterpret_cast<Message*>(message_buffer_.data());
    std::span<const char> payload(message_buffer_.data() + sizeof(Message), response->payload_size);
    if (launcher_->OnRecvHandshakeResponse(*response, payload)) {
        HLOG(INFO) << "Handshake done";
        UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_pipe_handle_),
                                   &EngineConnection::BufferAllocCallback,
                                   &EngineConnection::ReadMessageCallback));
        state_ = kRunning;
    }
}

void EngineConnection::WriteMessage(const Message& message) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_pipe_handle_.loop);
    uv_buf_t buf;
    launcher_->NewWriteBuffer(&buf);
    DCHECK_LE(sizeof(Message), buf.len);
    memcpy(buf.base, &message, sizeof(Message));
    buf.len = sizeof(Message);
    uv_write_t* write_req = launcher_->NewWriteRequest();
    write_req->data = buf.base;
    UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(&uv_pipe_handle_),
                          &buf, 1, &EngineConnection::WriteMessageCallback));
}

UV_CONNECT_CB_FOR_CLASS(EngineConnection, Connect) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to connect to engine, will close the connection: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    HLOG(INFO) << "Connected to engine, start writing handshake message";
    uv_buf_t buf = {
        .base = reinterpret_cast<char*>(&handshake_message_),
        .len = sizeof(Message)
    };
    UV_DCHECK_OK(uv_write(launcher_->NewWriteRequest(), UV_AS_STREAM(&uv_pipe_handle_),
                          &buf, 1, &EngineConnection::WriteHandshakeCallback));
}

UV_ALLOC_CB_FOR_CLASS(EngineConnection, BufferAlloc) {
    launcher_->NewReadBuffer(suggested_size, buf);
}

UV_READ_CB_FOR_CLASS(EngineConnection, ReadHandshakeResponse) {
    auto reclaim_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            launcher_->ReturnReadBuffer(buf);
        }
    });
    if (nread < 0) {
        HLOG(WARNING) << "Read error on handshake, will close the connection: "
                      << uv_strerror(nread);
        ScheduleClose();
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    message_buffer_.AppendData(buf->base, nread);
    if (message_buffer_.length() >= sizeof(Message)) {
        Message* response = reinterpret_cast<Message*>(message_buffer_.data());
        size_t expected_size = sizeof(Message) + response->payload_size;
        if (message_buffer_.length() >= expected_size) {
            RecvHandshakeResponse();
            message_buffer_.ConsumeFront(expected_size);
            while (message_buffer_.length() >= sizeof(Message)) {
                Message* message = reinterpret_cast<Message*>(message_buffer_.data());
                launcher_->OnRecvMessage(*message);
                message_buffer_.ConsumeFront(sizeof(Message));
            }
        }
    }
}

UV_WRITE_CB_FOR_CLASS(EngineConnection, WriteHandshake) {
    auto reclaim_resource = gsl::finally([this, req] {
        launcher_->ReturnWriteRequest(req);
    });
    if (status != 0) {
        HLOG(WARNING) << "Failed to write handshake message, will close the connection: "
                      << uv_strerror(status);
        ScheduleClose();
        return;
    }
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_pipe_handle_),
                               &EngineConnection::BufferAllocCallback,
                               &EngineConnection::ReadHandshakeResponseCallback));
}

UV_READ_CB_FOR_CLASS(EngineConnection, ReadMessage) {
    auto reclaim_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            launcher_->ReturnReadBuffer(buf);
        }
    });
    if (nread < 0) {
        HLOG(WARNING) << "Read error, will close the connection: "
                      << uv_strerror(nread);
        ScheduleClose();
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    utils::ReadMessages<Message>(
        &message_buffer_, buf->base, nread,
        [this] (Message* message) {
            launcher_->OnRecvMessage(*message);
        });
}

UV_WRITE_CB_FOR_CLASS(EngineConnection, WriteMessage) {
    auto reclaim_resource = gsl::finally([this, req] {
        launcher_->ReturnWriteBuffer(reinterpret_cast<char*>(req->data));
        launcher_->ReturnWriteRequest(req);
    });
    if (status != 0) {
        HLOG(WARNING) << "Failed to write response, will close the connection: "
                      << uv_strerror(status);
        ScheduleClose();
    }
}

UV_CLOSE_CB_FOR_CLASS(EngineConnection, Close) {
    state_ = kClosed;
    launcher_->OnEngineConnectionClose();
}

}  // namespace launcher
}  // namespace faas
