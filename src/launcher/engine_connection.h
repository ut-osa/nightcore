#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/uv.h"
#include "utils/appendable_buffer.h"
#include "utils/buffer_pool.h"
#include "utils/object_pool.h"

namespace faas {
namespace launcher {

class Launcher;

class EngineConnection : public uv::Base {
public:
    explicit EngineConnection(Launcher* launcher);
    ~EngineConnection();

    void Start(uv_loop_t* uv_loop, int engine_tcp_port,
               const protocol::Message& handshake_message);
    void ScheduleClose();

    void WriteMessage(const protocol::Message& message);

private:
    enum State { kCreated, kHandshake, kRunning, kClosing, kClosed };

    Launcher* launcher_;
    State state_;

    uv_connect_t connect_req_;
    uv_loop_t* uv_loop_;
    uv_stream_t* engine_conn_;

    utils::AppendableBuffer message_buffer_;
    protocol::Message handshake_message_;

    void RecvHandshakeResponse();

    DECLARE_UV_CONNECT_CB_FOR_CLASS(Connect);
    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadHandshakeResponse);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteHandshake);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadMessage);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteMessage);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(EngineConnection);
};

}  // namespace launcher
}  // namespace faas
