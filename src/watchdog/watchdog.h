#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "common/uv.h"
#include "utils/buffer_pool.h"
#include "watchdog/run_mode.h"
#include "watchdog/gateway_connection.h"
#include "watchdog/func_runner.h"
#include "watchdog/func_worker.h"

namespace faas {
namespace watchdog {

class Watchdog : public uv::Base {
public:
    static constexpr size_t kSubprocessPipeBufferSizeForSerializingMode = 65536;
    static constexpr size_t kSubprocessPipeBufferSizeForFuncWorkerMode = 256;
    static constexpr absl::Duration kMinFuncWorkerCreationInterval = absl::Milliseconds(500);

    Watchdog();
    ~Watchdog();

    void set_func_id(int func_id) {
        func_id_ = func_id;
    }
    void set_fprocess(std::string_view fprocess) {
        fprocess_ = std::string(fprocess);
    }
    void set_fprocess_working_dir(std::string_view path) {
        fprocess_working_dir_ = std::string(path);
    }
    void set_run_mode(int run_mode) {
        run_mode_ = gsl::narrow_cast<RunMode>(run_mode);
    }
    void set_min_num_func_workers(int value) {
        min_num_func_workers_ = value;
    }
    void set_max_num_func_workers(int value) {
        max_num_func_workers_ = value;
    }
    void set_func_worker_output_dir(std::string_view path) {
        func_worker_output_dir_ = std::string(path);
    }

    int func_id() const { return func_id_; }
    std::string_view fprocess() const { return fprocess_; }
    std::string_view fprocess_working_dir() const { return fprocess_working_dir_; }
    bool func_worker_async_mode() const { return run_mode_ == RunMode::FUNC_WORKER_ASYNC; }
    std::string_view func_worker_output_dir() const { return func_worker_output_dir_; }

    std::string_view func_name() const {
        const FuncConfig::Entry* entry = func_config_.find_by_func_id(func_id_);
        return entry->func_name;
    }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    void OnGatewayConnectionClose();
    void OnFuncRunnerComplete(FuncRunner* func_runner, FuncRunner::Status status,
                              int32_t processing_time);
    void OnFuncWorkerClose(FuncWorker* func_worker);
    void OnFuncWorkerIdle(FuncWorker* func_worker);
    stat::StatisticsCollector<int32_t>* func_worker_message_delay_stat() {
        return &func_worker_message_delay_stat_;
    }

    bool OnRecvHandshakeResponse(const protocol::Message& handshake_response,
                                 std::span<const char> payload);
    void OnRecvMessage(const protocol::Message& message);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    int func_id_;
    std::string fprocess_;
    std::string fprocess_working_dir_;
    RunMode run_mode_;
    int min_num_func_workers_;
    int max_num_func_workers_;
    std::string func_worker_output_dir_;
    uint16_t client_id_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    FuncConfig func_config_;

    GatewayConnection gateway_connection_;
    std::unique_ptr<utils::BufferPool> buffer_pool_for_subprocess_pipes_;
    absl::flat_hash_map<uint64_t, std::unique_ptr<FuncRunner>> func_runners_;

    std::vector<std::unique_ptr<FuncWorker>> func_workers_;
    absl::InlinedVector<FuncWorker*, 16> idle_func_workers_;
    int next_func_worker_id_;
    absl::BitGen random_bit_gen_;
    absl::Time last_func_worker_creation_time_;

    stat::StatisticsCollector<int32_t> gateway_message_delay_stat_;
    stat::StatisticsCollector<int32_t> func_worker_message_delay_stat_;
    stat::StatisticsCollector<int32_t> processing_delay_stat_;
    stat::CategoryCounter func_worker_decision_counter_;
    stat::CategoryCounter func_worker_load_counter_;
    stat::Counter incoming_requests_counter_;

    void EventLoopThreadMain();
    FuncWorker* PickFuncWorker();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);

    DISALLOW_COPY_AND_ASSIGN(Watchdog);
};

}  // namespace watchdog
}  // namespace faas
