#pragma once

#include "base/common.h"
#include "common/uv.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"
#include "server/connection_base.h"

namespace faas {
namespace engine {

class Engine;

class GatewayConnection final : public server::ConnectionBase {
public:
    static constexpr int kTypeId = 0;

    GatewayConnection(Engine* engine, uint16_t conn_id);
    ~GatewayConnection();

    uint16_t conn_id() const { return conn_id_; }

    uv_stream_t* InitUVHandle(uv_loop_t* uv_loop) override;
    void Start(server::IOWorker* io_worker) override;
    void ScheduleClose() override;

    void SendMessage(const protocol::GatewayMessage& message, std::span<const char> payload);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    Engine* engine_;
    uint16_t conn_id_;
    server::IOWorker* io_worker_;
    State state_;
    uv_tcp_t uv_tcp_handle_;

    std::string log_header_;

    utils::AppendableBuffer read_buffer_;

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(RecvData);
    DECLARE_UV_WRITE_CB_FOR_CLASS(DataWritten);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(GatewayConnection);
};

}  // namespace engine
}  // namespace faas
