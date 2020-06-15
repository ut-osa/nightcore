#pragma once

#include "base/common.h"
#include "common/stat.h"
#include "common/protocol.h"

namespace faas {
namespace gateway {

class Server;
class FuncWorker;

class Dispatcher {
public:
    Dispatcher(Server* server, uint16_t func_id);
    ~Dispatcher();

    int16_t func_id() const { return func_id_; }

    // All must be thread-safe
    bool OnFuncWorkerConnected(FuncWorker* func_worker);
    void OnFuncWorkerDisconnected(FuncWorker* func_worker);
    bool OnNewFuncCall(const protocol::FuncCall& func_call, size_t input_size,
                       std::span<const char> inline_input, bool shm_input);
    void OnFuncCallCompleted(const protocol::FuncCall& func_call,
                             int32_t processing_time, size_t output_size);
    void OnFuncCallFailed(const protocol::FuncCall& func_call);

private:
    Server* server_;
    uint16_t func_id_;
    absl::Mutex mu_;

    std::string log_header_;

    absl::flat_hash_map</* client_id */ uint16_t, FuncWorker*> workers_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* client_id */ uint16_t, protocol::FuncCall>
        running_workers_ ABSL_GUARDED_BY(mu_);
    std::vector</* client_id */ uint16_t> idle_workers_ ABSL_GUARDED_BY(mu_);
    std::queue<protocol::Message> pending_func_calls_ ABSL_GUARDED_BY(mu_);

    struct FuncCallState {
        int64_t recv_timestamp;
        int64_t dispatch_timestamp;
        FuncWorker* assigned_worker;
    };

    absl::flat_hash_map</* full_call_id */ uint64_t, FuncCallState>
        func_call_states_ ABSL_GUARDED_BY(mu_);

    stat::Counter incoming_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter failed_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint32_t> input_size_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint32_t> output_size_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<int32_t> queueing_delay_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<int32_t> processing_delay_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<int32_t> dispatch_overhead_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> idle_workers_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> running_workers_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> inflight_requests_stat_ ABSL_GUARDED_BY(mu_);

    void FuncWorkerFinished(FuncWorker* func_worker) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void DispatchFuncCall(FuncWorker* func_worker, protocol::Message* invoke_func_message)
        ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    bool DispatchPendingFuncCall(FuncWorker* idle_func_worker) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    FuncWorker* PickIdleWorker() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void UpdateWorkerLoadStat() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

    DISALLOW_COPY_AND_ASSIGN(Dispatcher);
};

}  // namespace gateway
}  // namespace faas
