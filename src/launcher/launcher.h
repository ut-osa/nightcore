#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "common/uv.h"
#include "utils/buffer_pool.h"
#include "launcher/gateway_connection.h"
#include "launcher/func_process.h"

namespace faas {
namespace launcher {

class Launcher : public uv::Base {
public:
    static constexpr size_t kSubprocessPipeBufferSize = 4096;

    Launcher();
    ~Launcher();

    void set_func_id(int func_id) {
        func_id_ = func_id;
    }
    void set_fprocess(std::string_view fprocess) {
        fprocess_ = std::string(fprocess);
    }
    void set_fprocess_working_dir(std::string_view path) {
        fprocess_working_dir_ = std::string(path);
    }
    void set_fprocess_output_dir(std::string_view path) {
        fprocess_output_dir_ = std::string(path);
    }
    void set_fprocess_multi_worker_mode(bool enable) {
        fprocess_multi_worker_mode_ = enable;
    }

    int func_id() const { return func_id_; }
    std::string_view fprocess() const { return fprocess_; }
    std::string_view fprocess_working_dir() const { return fprocess_working_dir_; }
    std::string_view fprocess_output_dir() const { return fprocess_output_dir_; }
    bool fprocess_multi_worker_mode() const { return fprocess_multi_worker_mode_; }

    std::string_view func_name() const {
        const FuncConfig::Entry* entry = func_config_.find_by_func_id(func_id_);
        return entry->func_name;
    }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    void OnEngineConnectionClose();
    void OnFuncProcessExit(FuncProcess* func_process);
    bool OnRecvHandshakeResponse(const protocol::Message& handshake_response,
                                 std::span<const char> payload);
    void OnRecvMessage(const protocol::Message& message);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    int func_id_;
    std::string fprocess_;
    std::string fprocess_working_dir_;
    std::string fprocess_output_dir_;
    bool fprocess_multi_worker_mode_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    FuncConfig func_config_;
    EngineConnection engine_connection_;
    std::unique_ptr<utils::BufferPool> buffer_pool_for_subprocess_pipes_;
    std::vector<std::unique_ptr<FuncProcess>> func_processes_;

    stat::StatisticsCollector<int32_t> engine_message_delay_stat_;

    void EventLoopThreadMain();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);

    DISALLOW_COPY_AND_ASSIGN(Launcher);
};

}  // namespace launcher
}  // namespace faas
