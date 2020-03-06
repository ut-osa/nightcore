#pragma once

#include "base/common.h"
#include "base/protocol.h"
#include "utils/uv_utils.h"
#include "utils/appendable_buffer.h"
#include "utils/buffer_pool.h"
#include "gateway/connection.h"

namespace faas {
namespace gateway {

class Server;

class MessageConnection : public Connection {
public:
    explicit MessageConnection(Server* server);
    ~MessageConnection();

    uv_stream_t* InitUVHandle(uv_loop_t* uv_loop) override;
    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

private:
    enum State { kCreated, kHandshake, kRunning, kClosing, kClosed };

    IOWorker* io_worker_;
    State state_;
    uv_pipe_t uv_pipe_handle_;

    std::string log_header_;

    utils::AppendableBuffer message_buffer_;
    protocol::HandshakeResponse handshake_response_;
    uv_write_t write_req_;

    void RecvHandshakeMessage();

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadHandshake);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteHandshakeResponse);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadMessage);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteResponse);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(MessageConnection);
};

}
}
