#pragma once

#include "base/common.h"
#include "base/protocol.h"
#include "utils/uv_utils.h"
#include "utils/appendable_buffer.h"
#include "utils/buffer_pool.h"
#include "utils/object_pool.h"

namespace faas {
namespace watchdog {

class Watchdog;

class GatewayPipe {
public:
    explicit GatewayPipe(Watchdog* watchdog);
    ~GatewayPipe();

    uv_pipe_t* uv_pipe_handle() { return &uv_pipe_handle_; }

    void Start(absl::string_view ipc_path, utils::BufferPool* buffer_pool,
               const protocol::HandshakeMessage& handshake_message);
    void ScheduleClose();

    void WriteWMessage(const protocol::Message& message);

private:
    enum State { kCreated, kHandshake, kRunning, kClosing, kClosed };

    Watchdog* watchdog_;
    State state_;

    uv_connect_t connect_req_;
    uv_pipe_t uv_pipe_handle_;

    utils::BufferPool* buffer_pool_;
    utils::AppendableBuffer message_buffer_;
    protocol::HandshakeMessage handshake_message_;
    utils::SimpleObjectPool<uv_write_t> write_req_pool_;

    void RecvHandshakeResponse();

    DECLARE_UV_CONNECT_CB_FOR_CLASS(Connect);
    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadHandshakeResponse);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteHandshake);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadMessage);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteMessage);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(GatewayPipe);
};

}  // namespace watchdog
}  // namespace faas
