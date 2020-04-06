#pragma once

#include "base/common.h"
#include "common/uv.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "utils/appendable_buffer.h"
#include "utils/object_pool.h"
#include "gateway/connection.h"

namespace faas {
namespace gateway {

class Server;

class MessageConnection final : public Connection {
public:
    explicit MessageConnection(Server* server);
    ~MessageConnection();

    protocol::Role role() const { return role_; }
    uint16_t func_id() const { return func_id_; }
    uint16_t client_id() const { return client_id_; }

    uv_stream_t* InitUVHandle(uv_loop_t* uv_loop) override;
    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    // Must be thread-safe
    void WriteMessage(const protocol::Message& message);

private:
    enum State { kCreated, kHandshake, kRunning, kClosing, kClosed };

    IOWorker* io_worker_;
    State state_;
    protocol::Role role_;
    uint16_t func_id_;
    uint16_t client_id_;
    uv_pipe_t uv_pipe_handle_;

    std::string log_header_;

    utils::AppendableBuffer message_buffer_;
    protocol::HandshakeResponse handshake_response_;
    utils::AppendableBuffer write_message_buffer_;

    absl::Mutex write_message_mu_;
    absl::InlinedVector<protocol::Message, 16>
        pending_messages_ ABSL_GUARDED_BY(write_message_mu_);

    void RecvHandshakeMessage();
    void SendPendingMessages();

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadHandshake);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteHandshakeResponse);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadMessage);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteMessage);
    DECLARE_UV_ASYNC_CB_FOR_CLASS(NewMessageForWrite);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(MessageConnection);
};

}  // namespace gateway
}  // namespace faas
