#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "utils/object_pool.h"
#include "utils/exp_moving_avg.h"

namespace faas {
namespace engine {

class Engine;
class FuncWorker;

class Tracer {
public:
    explicit Tracer(Engine* engine);
    ~Tracer();

    void Init();

    enum class FuncCallState {
        kInvalid,
        kReceived,
        kDispatched,
        kCompleted,
        kFailed
    };

    struct FuncCallInfo {
        absl::Mutex        mu;
        FuncCallState      state               ABSL_GUARDED_BY(mu);
        protocol::FuncCall func_call           ABSL_GUARDED_BY(mu);
        protocol::FuncCall parent_func_call    ABSL_GUARDED_BY(mu);
        size_t             input_size          ABSL_GUARDED_BY(mu);
        size_t             output_size         ABSL_GUARDED_BY(mu);
        int64_t            recv_timestamp      ABSL_GUARDED_BY(mu);
        int64_t            dispatch_timestamp  ABSL_GUARDED_BY(mu);
        int64_t            finish_timestamp    ABSL_GUARDED_BY(mu);
        uint16_t           assigned_worker     ABSL_GUARDED_BY(mu);  // Saved as client_id
        int32_t            processing_time     ABSL_GUARDED_BY(mu);
        int32_t            dispatch_delay      ABSL_GUARDED_BY(mu);
        int32_t            total_queuing_delay ABSL_GUARDED_BY(mu);
    };

    FuncCallInfo* OnNewFuncCall(const protocol::FuncCall& func_call,
                                const protocol::FuncCall& parent_func_call,
                                size_t input_size);
    FuncCallInfo* OnFuncCallDispatched(const protocol::FuncCall& func_call,
                                       FuncWorker* func_worker);
    FuncCallInfo* OnFuncCallCompleted(const protocol::FuncCall& func_call,
                                      int32_t processing_time, int32_t dispatch_delay,
                                      size_t output_size);
    FuncCallInfo* OnFuncCallFailed(const protocol::FuncCall& func_call, int32_t dispatch_delay);

    void DiscardFuncCallInfo(const protocol::FuncCall& func_call);

    double GetAverageInstantRps(uint16_t func_id);
    double GetAverageRunningDelay(uint16_t func_id);
    double GetAverageProcessingTime(uint16_t func_id);
    double GetAverageProcessingTime2(uint16_t func_id);

private:
    Engine* engine_;
    absl::Mutex mu_;

    utils::SimpleObjectPool<FuncCallInfo> func_call_info_pool_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* full_call_id */ uint64_t, FuncCallInfo*>
        func_call_infos_ ABSL_GUARDED_BY(mu_);

    stat::StatisticsCollector<int32_t> dispatch_delay_stat_    ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<int32_t> dispatch_overhead_stat_ ABSL_GUARDED_BY(mu_);

    struct PerFuncStatistics {
        absl::Mutex mu;
        uint16_t    func_id;

        int      inflight_requests      ABSL_GUARDED_BY(mu);
        int64_t  last_request_timestamp ABSL_GUARDED_BY(mu);

        stat::Counter                       incoming_requests_stat ABSL_GUARDED_BY(mu);
        stat::Counter                       failed_requests_stat   ABSL_GUARDED_BY(mu);
        stat::StatisticsCollector<float>    instant_rps_stat       ABSL_GUARDED_BY(mu);
        stat::StatisticsCollector<uint32_t> input_size_stat        ABSL_GUARDED_BY(mu);
        stat::StatisticsCollector<uint32_t> output_size_stat       ABSL_GUARDED_BY(mu);
        stat::StatisticsCollector<int32_t>  queueing_delay_stat    ABSL_GUARDED_BY(mu);
        stat::StatisticsCollector<int32_t>  running_delay_stat     ABSL_GUARDED_BY(mu);
        stat::StatisticsCollector<uint16_t> inflight_requests_stat ABSL_GUARDED_BY(mu);

        utils::ExpMovingAvgExt instant_rps_ema      ABSL_GUARDED_BY(mu);
        utils::ExpMovingAvg    running_delay_ema    ABSL_GUARDED_BY(mu);
        utils::ExpMovingAvg    processing_time_ema  ABSL_GUARDED_BY(mu);
        utils::ExpMovingAvg    processing_time2_ema ABSL_GUARDED_BY(mu);

        explicit PerFuncStatistics(uint16_t func_id);
    };
    PerFuncStatistics* per_func_stats_[protocol::kMaxFuncId];

    DISALLOW_COPY_AND_ASSIGN(Tracer);
};

}  // namespace engine
}  // namespace faas
