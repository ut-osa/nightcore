#pragma once

#include "base/common.h"
#include "base/protocol.h"
#include "utils/uv_utils.h"
#include "utils/appendable_buffer.h"
#include "utils/buffer_pool.h"

namespace faas {
namespace gateway {

class Server;

// Well, although called WatchdogPipe, it's actually backed by
// Unix domain socket for bidirectional communication
class WatchdogPipe {
public:
    explicit WatchdogPipe(Server* server);
    ~WatchdogPipe();

    uv_pipe_t* uv_pipe_handle() { return &uv_pipe_handle_; }
    const std::string& function_name() const { return function_name_; }

    void Start(utils::BufferPool* buffer_pool);
    void ScheduleClose();

private:
    enum State { kCreated, kHandshake, kRunning, kClosing, kClosed };

    Server* server_;
    State state_;
    uv_pipe_t uv_pipe_handle_;
    std::string function_name_;

    std::string log_header_;

    utils::BufferPool* buffer_pool_;
    utils::AppendableBuffer message_buffer_;
    protocol::WatchdogHandshakeResponse handshake_response_;
    uv_write_t write_req_;

    void RecvHandshakeMessage();

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadHandshake);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteHandshakeResponse);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadMessage);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteResponse);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(WatchdogPipe);
};

}
}
