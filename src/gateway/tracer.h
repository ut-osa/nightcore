#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "utils/object_pool.h"
#include "utils/exp_moving_avg.h"

namespace faas {
namespace gateway {

class Server;
class FuncWorker;

class Tracer {
public:
    explicit Tracer(Server* server);
    ~Tracer();

    void Init();

    void OnNewFuncCall(const protocol::FuncCall& func_call,
                       const protocol::FuncCall& parent_func_call,
                       size_t input_size);
    void OnFuncCallDispatched(const protocol::FuncCall& func_call, FuncWorker* func_worker);
    void OnFuncCallCompleted(const protocol::FuncCall& func_call,
                             int32_t processing_time, size_t output_size);
    void OnFuncCallFailed(const protocol::FuncCall& func_call);

private:
    Server* server_;
    absl::Mutex mu_;

    enum FuncCallState { kInvalid, kReceived, kDispatched, kCompleted, kFailed };

    struct FunCallInfo {
        FuncCallState      state;
        protocol::FuncCall func_call;
        protocol::FuncCall parent_func_call;
        size_t             input_size;
        size_t             output_size;
        int64_t            recv_timestamp;
        int64_t            dispatch_timestamp;
        int64_t            finish_timestamp;
        FuncWorker*        assigned_worker;
        int32_t            processing_time;

        void reset() {
            state              = kInvalid;
            func_call          = protocol::kInvalidFuncCall;
            parent_func_call   = protocol::kInvalidFuncCall;
            input_size         = 0;
            output_size        = 0;
            recv_timestamp     = 0;
            dispatch_timestamp = 0;
            finish_timestamp   = 0;
            assigned_worker    = nullptr;
            processing_time    = 0;
        }
    };
    utils::SimpleObjectPool<FunCallInfo> func_call_info_pool_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* full_call_id */ uint64_t, FunCallInfo*>
        func_call_infos_ ABSL_GUARDED_BY(mu_);

    struct PerFuncState {
        uint16_t func_id;
        int      inflight_requests;
        int64_t  last_request_timestamp;

        // per function statistics
        stat::Counter                       incoming_requests_stat;
        stat::Counter                       failed_requests_stat;
        stat::StatisticsCollector<float>    instant_rps_stat;
        stat::StatisticsCollector<uint32_t> input_size_stat;
        stat::StatisticsCollector<uint32_t> output_size_stat;
        stat::StatisticsCollector<int32_t>  queueing_delay_stat;
        stat::StatisticsCollector<int32_t>  running_delay_stat;
        stat::StatisticsCollector<uint16_t> inflight_requests_stat;

        utils::ExpMovingAvg rps_ema;
        utils::ExpMovingAvg running_delay_ema;
        utils::ExpMovingAvg processing_time_ema;

        explicit PerFuncState(uint16_t func_id);
    };
    PerFuncState* per_func_state_[protocol::kMaxFuncId];

    stat::StatisticsCollector<int32_t> dispatch_overhead_stat_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(Tracer);
};

}  // namespace gateway
}  // namespace faas
