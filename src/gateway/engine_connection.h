#pragma once

#include "base/common.h"
#include "common/uv.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"
#include "server/connection_base.h"

namespace faas {
namespace gateway {

class Server;

class EngineConnection final : public server::ConnectionBase {
public:
    static constexpr int kBaseTypeId = 2;

    static int type_id(uint16_t node_id) { return kBaseTypeId + node_id; }

    EngineConnection(Server* server, uint16_t node_id, uint16_t conn_id,
                     std::span<const char> initial_data);
    ~EngineConnection();

    uint16_t node_id() const { return node_id_; }
    uint16_t conn_id() const { return conn_id_; }

    uv_stream_t* InitUVHandle(uv_loop_t* uv_loop) override;
    void Start(server::IOWorker* io_worker) override;
    void ScheduleClose() override;

    void SendMessage(const protocol::GatewayMessage& message, std::span<const char> payload);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    Server* server_;
    uint16_t node_id_;
    uint16_t conn_id_;
    server::IOWorker* io_worker_;
    State state_;
    uv_tcp_t uv_tcp_handle_;

    std::string log_header_;

    utils::AppendableBuffer read_buffer_;

    void ProcessGatewayMessages();

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(RecvData);
    DECLARE_UV_WRITE_CB_FOR_CLASS(DataSent);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(EngineConnection);
};

}  // namespace gateway
}  // namespace faas
