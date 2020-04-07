#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "common/uv.h"
#include "utils/shared_memory.h"
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
    static constexpr int kDefaultGoMaxProcs = 1;

    Watchdog();
    ~Watchdog();

    void set_gateway_ipc_path(std::string_view path) {
        gateway_ipc_path_ = std::string(path);
    }
    void set_func_id(int func_id) {
        func_id_ = func_id;
    }
    void set_fprocess(std::string_view fprocess) {
        fprocess_ = std::string(fprocess);
    }
    void set_fprocess_working_dir(std::string_view path) {
        fprocess_working_dir_ = std::string(path);
    }
    void set_shared_mem_path(std::string_view path) {
        shared_mem_path_ = std::string(path);
    }
    void set_func_config_file(std::string_view path) {
        func_config_file_ = std::string(path);
    }
    void set_run_mode(int run_mode) {
        run_mode_ = static_cast<RunMode>(run_mode);
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
    void set_go_max_procs(int value) {
        go_max_procs_ = value;
    }

    std::string_view gateway_ipc_path() const { return gateway_ipc_path_; }
    int func_id() const { return func_id_; }
    std::string_view fprocess() const { return fprocess_; }
    std::string_view fprocess_working_dir() const { return fprocess_working_dir_; }
    std::string_view shared_mem_path() const { return shared_mem_path_; }
    std::string_view func_config_file() const { return func_config_file_; }
    std::string_view func_worker_output_dir() const { return func_worker_output_dir_; }
    int go_max_procs() const { return go_max_procs_; }

    std::string_view func_name() const {
        const FuncConfig::Entry* entry = func_config_.find_by_func_id(func_id_);
        return entry->func_name;
    }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    void OnGatewayConnectionClose();
    void OnFuncRunnerComplete(FuncRunner* func_runner, FuncRunner::Status status, uint32_t processing_time);
    void OnFuncWorkerClose(FuncWorker* func_worker);
    void OnFuncWorkerIdle(FuncWorker* func_worker);
    stat::StatisticsCollector<uint32_t>* func_worker_message_delay_stat() {
        return &func_worker_message_delay_stat_;
    }

    bool OnRecvHandshakeResponse(const protocol::HandshakeResponse& response);
    void OnRecvMessage(const protocol::Message& message);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    std::string gateway_ipc_path_;
    int func_id_;
    std::string fprocess_;
    std::string fprocess_working_dir_;
    std::string shared_mem_path_;
    std::string func_config_file_;
    RunMode run_mode_;
    int min_num_func_workers_;
    int max_num_func_workers_;
    std::string func_worker_output_dir_;
    int go_max_procs_;
    uint16_t client_id_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    FuncConfig func_config_;
    std::unique_ptr<utils::SharedMemory> shared_memory_;

    GatewayConnection gateway_connection_;
    std::unique_ptr<utils::BufferPool> buffer_pool_for_subprocess_pipes_;
    absl::flat_hash_map<uint64_t, std::unique_ptr<FuncRunner>> func_runners_;

    std::vector<std::unique_ptr<FuncWorker>> func_workers_;
    absl::InlinedVector<FuncWorker*, 16> idle_func_workers_;
    int next_func_worker_id_;
    absl::BitGen random_bit_gen_;
    absl::Time last_func_worker_creation_time_;

    stat::StatisticsCollector<uint32_t> gateway_message_delay_stat_;
    stat::StatisticsCollector<uint32_t> func_worker_message_delay_stat_;
    stat::StatisticsCollector<uint32_t> processing_delay_stat_;
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
