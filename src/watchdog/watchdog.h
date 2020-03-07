#pragma once

#include "base/common.h"
#include "base/protocol.h"
#include "utils/uv_utils.h"
#include "utils/buffer_pool.h"
#include "watchdog/gateway_pipe.h"

namespace faas {
namespace watchdog {

class Watchdog {
public:
    static constexpr size_t kPipeBufferSize = 256;

    Watchdog();
    ~Watchdog();

    void set_gateway_ipc_path(absl::string_view path) {
        gateway_ipc_path_ = std::string(path);
    }
    void set_func_id(int func_id) {
        func_id_ = func_id;
    }
    void set_fprocess(absl::string_view fprocess) {
        fprocess_ = std::string(fprocess);
    }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    void OnGatewayPipeClose();

    bool OnRecvHandshakeResponse(const protocol::HandshakeResponse& response);
    void OnRecvMessage(const protocol::Message& message);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    std::string gateway_ipc_path_;
    int func_id_;
    std::string fprocess_;
    uint16_t client_id_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    GatewayPipe gateway_pipe_;
    utils::BufferPool buffer_pool_for_pipes_;

    void EventLoopThreadMain();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);

    DISALLOW_COPY_AND_ASSIGN(Watchdog);
};

}  // namespace watchdog
}  // namespace faas
