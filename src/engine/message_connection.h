#pragma once

#include "base/common.h"
#include "common/uv.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "utils/appendable_buffer.h"
#include "utils/object_pool.h"
#include "server/io_worker.h"
#include "server/connection_base.h"

namespace faas {
namespace engine {

class Engine;

class MessageConnection final : public server::ConnectionBase {
public:
    static constexpr int kTypeId = 1;

    explicit MessageConnection(Engine* engine);
    ~MessageConnection();

    uint16_t func_id() const { return func_id_; }
    uint16_t client_id() const { return client_id_; }
    bool handshake_done() const { return handshake_done_; }
    bool is_launcher_connection() const { return client_id_ == 0; }
    bool is_func_worker_connection() const { return client_id_ > 0; }

    uv_stream_t* InitUVHandle(uv_loop_t* uv_loop) override;
    void Start(server::IOWorker* io_worker) override;
    void ScheduleClose() override;

    // Must be thread-safe
    void WriteMessage(const protocol::Message& message);

private:
    enum State { kCreated, kHandshake, kRunning, kClosing, kClosed };

    Engine* engine_;
    server::IOWorker* io_worker_;
    State state_;
    uint16_t func_id_;
    uint16_t client_id_;
    bool handshake_done_;
    uv_stream_t* uv_handle_;

    uv_pipe_t in_fifo_handle_;
    uv_pipe_t out_fifo_handle_;
    uv_stream_t* handle_for_read_message_;
    uv_stream_t* handle_for_write_message_;
    uv::HandleScope handle_scope_;
    std::atomic<int> pipe_for_write_fd_;

    std::string log_header_;

    utils::AppendableBuffer message_buffer_;
    protocol::Message handshake_response_;
    utils::AppendableBuffer write_message_buffer_;

    absl::Mutex write_message_mu_;
    absl::InlinedVector<protocol::Message, 16>
        pending_messages_ ABSL_GUARDED_BY(write_message_mu_);

    void RecvHandshakeMessage();
    void SendPendingMessages();
    void OnAllHandlesClosed();

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadHandshake);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteHandshakeResponse);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadMessage);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteMessage);

    DISALLOW_COPY_AND_ASSIGN(MessageConnection);
};

}  // namespace engine
}  // namespace faas
