#pragma once

#include "base/common.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "utils/object_pool.h"
#include "engine/tracer.h"

namespace faas {
namespace engine {

class Engine;
class FuncWorker;

class Dispatcher {
public:
    Dispatcher(Engine* engine, uint16_t func_id);
    ~Dispatcher();

    int16_t func_id() const { return func_id_; }

    // All must be thread-safe
    bool OnFuncWorkerConnected(std::shared_ptr<FuncWorker> func_worker);
    void OnFuncWorkerDisconnected(FuncWorker* func_worker);
    bool OnNewFuncCall(const protocol::FuncCall& func_call,
                       const protocol::FuncCall& parent_func_call,
                       size_t input_size, std::span<const char> inline_input, bool shm_input);
    bool OnFuncCallCompleted(const protocol::FuncCall& func_call,
                             int32_t processing_time, int32_t dispatch_delay, size_t output_size);
    bool OnFuncCallFailed(const protocol::FuncCall& func_call, int32_t dispatch_delay);

private:
    Engine* engine_;
    uint16_t func_id_;
    const FuncConfig::Entry* func_config_entry_;
    size_t min_workers_;
    size_t max_workers_;
    absl::Mutex mu_;

    std::string log_header_;

    absl::flat_hash_map</* client_id */ uint16_t, std::shared_ptr<FuncWorker>>
        workers_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* client_id */ uint16_t, protocol::FuncCall>
        running_workers_ ABSL_GUARDED_BY(mu_);
    std::vector</* client_id */ uint16_t> idle_workers_ ABSL_GUARDED_BY(mu_);
    utils::SimpleObjectPool<protocol::Message> message_pool_ ABSL_GUARDED_BY(mu_);

    absl::flat_hash_map</* client_id */ uint16_t, /* request_timestamp */ int64_t>
        requested_workers_ ABSL_GUARDED_BY(mu_);
    int64_t last_request_worker_timestamp_ ABSL_GUARDED_BY(mu_);

    struct PendingFuncCall {
        protocol::Message*    dispatch_func_call_message;
        Tracer::FuncCallInfo* func_call_info;
    };

    std::queue<PendingFuncCall> pending_func_calls_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* full_call_id */ uint64_t, /* client_id */ uint16_t>
        assigned_workers_ ABSL_GUARDED_BY(mu_);

    stat::StatisticsCollector<uint16_t> idle_workers_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> running_workers_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint32_t> max_concurrency_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<float> estimated_rps_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<float> estimated_concurrency_stat_ ABSL_GUARDED_BY(mu_);

    void FuncWorkerFinished(FuncWorker* func_worker) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void DispatchFuncCall(FuncWorker* func_worker, protocol::Message* dispatch_func_call_message)
        ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    bool DispatchPendingFuncCall(FuncWorker* idle_func_worker) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    FuncWorker* PickIdleWorker() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void UpdateWorkerLoadStat() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    size_t DetermineExpectedConcurrency() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    size_t DetermineConcurrencyLimit() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void MayRequestNewFuncWorker() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

    DISALLOW_COPY_AND_ASSIGN(Dispatcher);
};

}  // namespace engine
}  // namespace faas
