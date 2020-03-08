#pragma once

#include "base/common.h"
#include "base/protocol.h"
#include "utils/uv_utils.h"
#include "utils/shared_memory.h"
#include "utils/buffer_pool.h"
#include "watchdog/run_mode.h"
#include "watchdog/gateway_connection.h"
#include "watchdog/func_worker.h"

namespace faas {
namespace watchdog {

class Watchdog {
public:
    static constexpr size_t kDataPipeBufferSize = 65536;

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
    void set_shared_mem_path(absl::string_view shared_mem_path) {
        shared_mem_path_ = std::string(shared_mem_path);
    }
    void set_run_mode(int run_mode) {
        run_mode_ = static_cast<RunMode>(run_mode);
    }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    void OnGatewayConnectionClose();
    void OnFuncWorkerExit(FuncWorker* func_worker);

    bool OnRecvHandshakeResponse(const protocol::HandshakeResponse& response);
    void OnRecvMessage(const protocol::Message& message);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    std::string gateway_ipc_path_;
    int func_id_;
    std::string fprocess_;
    std::string shared_mem_path_;
    RunMode run_mode_;
    uint16_t client_id_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    std::unique_ptr<utils::SharedMemory> shared_memory_;

    GatewayConnection gateway_connection_;
    int next_func_worker_id_;
    absl::flat_hash_set<std::unique_ptr<FuncWorker>> func_workers_;
    utils::BufferPool buffer_pool_for_data_pipes_;

    void EventLoopThreadMain();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);

    DISALLOW_COPY_AND_ASSIGN(Watchdog);
};

}  // namespace watchdog
}  // namespace faas
