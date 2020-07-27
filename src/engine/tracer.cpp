#include "engine/tracer.h"

#include "engine/engine.h"
#include "engine/worker_manager.h"

#include <absl/flags/flag.h>

#define HLOG(l) LOG(l) << "Tracer: "
#define HVLOG(l) VLOG(l) << "Tracer: "

ABSL_FLAG(double, instant_rps_p_norm, 1.0, "");
ABSL_FLAG(double, instant_rps_ema_alpha, 0.001, "");
ABSL_FLAG(double, instant_rps_ema_tau_ms, 0, "");

namespace faas {
namespace engine {

using protocol::FuncCall;
using protocol::FuncCallDebugString;

Tracer::Tracer(Engine* engine)
    : engine_(engine),
      dispatch_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("dispatch_delay")),
      dispatch_overhead_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("dispatch_overhead")) {
    for (int i = 0; i < protocol::kMaxFuncId; i++) {
        per_func_stats_[i] = nullptr;
    }
}

Tracer::~Tracer() {
    for (int i = 0; i < protocol::kMaxFuncId; i++) {
        if (per_func_stats_[i] != nullptr) {
            delete per_func_stats_[i];
        }
    }
}

void Tracer::Init() {
    for (int i = 0; i < protocol::kMaxFuncId; i++) {
        if (engine_->func_config()->find_by_func_id(i) != nullptr) {
            per_func_stats_[i] = new PerFuncStatistics(i);
        }
    }
}

Tracer::FuncCallInfo* Tracer::OnNewFuncCall(const FuncCall& func_call,
                                            const FuncCall& parent_func_call, size_t input_size) {
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    FuncCallInfo* info;

    {
        absl::MutexLock lk(&mu_);
        if (func_call_infos_.contains(func_call.full_call_id)) {
            HLOG(WARNING) << "FuncCall already exists: " << FuncCallDebugString(func_call);
            return nullptr;
        }
        info = func_call_info_pool_.Get();
        func_call_infos_[func_call.full_call_id] = info;
    }

    {
        absl::MutexLock lk(&info->mu);
        info->state = FuncCallState::kReceived;
        info->func_call = func_call;
        info->parent_func_call = parent_func_call;
        info->input_size = input_size;
        info->output_size = 0;
        info->recv_timestamp = current_timestamp;
        info->dispatch_timestamp = 0;
        info->finish_timestamp = 0;
        info->assigned_worker = 0;
        info->processing_time = 0;
        info->total_queuing_delay = 0;
    }

    uint16_t func_id = func_call.func_id;
    DCHECK_LT(func_id, protocol::kMaxFuncId);
    DCHECK(per_func_stats_[func_id] != nullptr);
    PerFuncStatistics* per_func_stat = per_func_stats_[func_id];
    {
        absl::MutexLock lk(&per_func_stat->mu);
        int64_t current_timestamp = GetMonotonicMicroTimestamp();
        if (current_timestamp <= per_func_stat->last_request_timestamp) {
            current_timestamp = per_func_stat->last_request_timestamp + 1;
        }
        if (per_func_stat->last_request_timestamp != -1) {
            double instant_rps = gsl::narrow_cast<double>(
                1e6 / (current_timestamp - per_func_stat->last_request_timestamp));
            per_func_stat->instant_rps_stat.AddSample(instant_rps);
            per_func_stat->instant_rps_ema.AddSample(current_timestamp, instant_rps);
        }
        per_func_stat->last_request_timestamp = current_timestamp;
        per_func_stat->inflight_requests++;
        per_func_stat->inflight_requests_stat.AddSample(
            gsl::narrow_cast<uint16_t>(per_func_stat->inflight_requests));
        per_func_stat->incoming_requests_stat.Tick();
        per_func_stat->input_size_stat.AddSample(gsl::narrow_cast<uint32_t>(input_size));
    }

    return info;
}

Tracer::FuncCallInfo* Tracer::OnFuncCallDispatched(const protocol::FuncCall& func_call,
                                                   FuncWorker* func_worker) {
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    FuncCallInfo* info;

    {
        absl::MutexLock lk(&mu_);
        if (!func_call_infos_.contains(func_call.full_call_id)) {
            HLOG(WARNING) << "Cannot find FuncCall: " << FuncCallDebugString(func_call);
            return nullptr;
        }
        info = func_call_infos_[func_call.full_call_id];
    }

    int32_t queueing_delay = 0;
    {
        absl::MutexLock lk(&info->mu);
        info->state = FuncCallState::kDispatched;
        info->dispatch_timestamp = current_timestamp;
        info->assigned_worker = func_worker->client_id();
        queueing_delay = gsl::narrow_cast<int32_t>(current_timestamp - info->recv_timestamp);
        info->total_queuing_delay += queueing_delay;
    }

    PerFuncStatistics* per_func_stat = per_func_stats_[func_call.func_id];
    {
        absl::MutexLock lk(&per_func_stat->mu);
        absl::ReaderMutexLock info_lk(&info->mu);
        per_func_stat->queueing_delay_stat.AddSample(queueing_delay);
    }

    return info;
}

Tracer::FuncCallInfo* Tracer::OnFuncCallCompleted(const FuncCall& func_call,
                                                  int32_t processing_time, int32_t dispatch_delay,
                                                  size_t output_size) {
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    FuncCallInfo* info;
    FuncCallInfo* parent_info = nullptr;

    {
        absl::MutexLock lk(&mu_);
        if (!func_call_infos_.contains(func_call.full_call_id)) {
            HLOG(WARNING) << "Cannot find FuncCall: " << FuncCallDebugString(func_call);
            return nullptr;
        }
        info = func_call_infos_[func_call.full_call_id];
        absl::ReaderMutexLock info_lk(&info->mu);
        dispatch_overhead_stat_.AddSample(gsl::narrow_cast<int32_t>(
            current_timestamp - info->dispatch_timestamp - processing_time));
        dispatch_delay_stat_.AddSample(dispatch_delay);
        if (func_call_infos_.contains(info->parent_func_call.full_call_id)) {
            parent_info = func_call_infos_[info->parent_func_call.full_call_id];
        }
    }

    int32_t total_queuing_delay;
    {
        absl::MutexLock lk(&info->mu);
        info->state = FuncCallState::kCompleted;
        info->output_size = output_size;
        info->finish_timestamp = current_timestamp;
        info->processing_time = processing_time;
        info->dispatch_delay = dispatch_delay;
        total_queuing_delay = info->total_queuing_delay;
    }
    if (parent_info != nullptr) {
        absl::MutexLock lk(&parent_info->mu);
        parent_info->total_queuing_delay += total_queuing_delay;
    }

    PerFuncStatistics* per_func_stat = per_func_stats_[func_call.func_id];
    {
        absl::MutexLock lk(&per_func_stat->mu);
        absl::ReaderMutexLock info_lk(&info->mu);
        int32_t running_delay = gsl::narrow_cast<int32_t>(
            current_timestamp - info->dispatch_timestamp);
        per_func_stat->running_delay_stat.AddSample(running_delay);
        per_func_stat->inflight_requests--;
        if (per_func_stat->inflight_requests < 0) {
            HLOG(ERROR) << "Negative inflight_requests for func_id " << per_func_stat->func_id;
            per_func_stat->inflight_requests = 0;
        }
        per_func_stat->output_size_stat.AddSample(gsl::narrow_cast<uint32_t>(output_size));
        per_func_stat->running_delay_ema.AddSample(running_delay);
        per_func_stat->processing_time_ema.AddSample(processing_time);
        int64_t processing_time2 = current_timestamp - info->recv_timestamp - total_queuing_delay;
        if (processing_time2 > 0) {
            per_func_stat->processing_time2_ema.AddSample(
                gsl::narrow_cast<int32_t>(processing_time2));
        }
    }

    return info;
}

Tracer::FuncCallInfo* Tracer::OnFuncCallFailed(const FuncCall& func_call, int32_t dispatch_delay) {
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    FuncCallInfo* info;
    FuncCallInfo* parent_info = nullptr;

    {
        absl::MutexLock lk(&mu_);
        if (!func_call_infos_.contains(func_call.full_call_id)) {
            HLOG(WARNING) << "Cannot find FuncCall: " << FuncCallDebugString(func_call);
            return nullptr;
        }
        info = func_call_infos_[func_call.full_call_id];
        absl::ReaderMutexLock info_lk(&info->mu);
        dispatch_delay_stat_.AddSample(dispatch_delay);
        if (func_call_infos_.contains(info->parent_func_call.full_call_id)) {
            parent_info = func_call_infos_[info->parent_func_call.full_call_id];
        }
    }

    int32_t total_queuing_delay;
    {
        absl::MutexLock lk(&info->mu);
        info->state = FuncCallState::kFailed;
        info->finish_timestamp = current_timestamp;
        info->dispatch_delay = dispatch_delay;
        total_queuing_delay = info->total_queuing_delay;
    }
    if (parent_info != nullptr) {
        absl::MutexLock lk(&parent_info->mu);
        parent_info->total_queuing_delay += total_queuing_delay;
    }

    PerFuncStatistics* per_func_stat = per_func_stats_[func_call.func_id];
    {
        absl::MutexLock lk(&per_func_stat->mu);
        per_func_stat->failed_requests_stat.Tick();
        per_func_stat->inflight_requests--;
        if (per_func_stat->inflight_requests < 0) {
            HLOG(ERROR) << "Negative inflight_requests for func_id " << per_func_stat->func_id;
            per_func_stat->inflight_requests = 0;
        }
    }

    return info;
}

void Tracer::DiscardFuncCallInfo(const protocol::FuncCall& func_call) {
    absl::MutexLock lk(&mu_);
    if (!func_call_infos_.contains(func_call.full_call_id)) {
        HLOG(WARNING) << "Cannot find FuncCall: " << FuncCallDebugString(func_call);
        return;
    }
    func_call_infos_.erase(func_call.full_call_id);
}

double Tracer::GetAverageInstantRps(uint16_t func_id) {
    DCHECK_LT(func_id, protocol::kMaxFuncId);
    DCHECK(per_func_stats_[func_id] != nullptr);
    PerFuncStatistics* per_func_stat = per_func_stats_[func_id];
    {
        absl::MutexLock lk(&per_func_stat->mu);
        return per_func_stat->instant_rps_ema.GetValue();
    }
}

double Tracer::GetAverageRunningDelay(uint16_t func_id) {
    DCHECK_LT(func_id, protocol::kMaxFuncId);
    DCHECK(per_func_stats_[func_id] != nullptr);
    PerFuncStatistics* per_func_stat = per_func_stats_[func_id];
    {
        absl::MutexLock lk(&per_func_stat->mu);
        return per_func_stat->running_delay_ema.GetValue();
    }
}

double Tracer::GetAverageProcessingTime(uint16_t func_id) {
    DCHECK_LT(func_id, protocol::kMaxFuncId);
    DCHECK(per_func_stats_[func_id] != nullptr);
    PerFuncStatistics* per_func_stat = per_func_stats_[func_id];
    {
        absl::MutexLock lk(&per_func_stat->mu);
        return per_func_stat->processing_time_ema.GetValue();
    }
}

double Tracer::GetAverageProcessingTime2(uint16_t func_id) {
    DCHECK_LT(func_id, protocol::kMaxFuncId);
    DCHECK(per_func_stats_[func_id] != nullptr);
    PerFuncStatistics* per_func_stat = per_func_stats_[func_id];
    {
        absl::MutexLock lk(&per_func_stat->mu);
        return per_func_stat->processing_time2_ema.GetValue();
    }
}

Tracer::PerFuncStatistics::PerFuncStatistics(uint16_t func_id)
    : func_id(func_id),
      inflight_requests(0),
      last_request_timestamp(-1),
      incoming_requests_stat(stat::Counter::StandardReportCallback(
          fmt::format("incoming_requests[{}]", func_id))),
      failed_requests_stat(stat::Counter::StandardReportCallback(
          fmt::format("failed_requests[{}]", func_id))),
      instant_rps_stat(stat::StatisticsCollector<float>::StandardReportCallback(
          fmt::format("instant_rps[{}]", func_id))),
      input_size_stat(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          fmt::format("input_size[{}]", func_id))),
      output_size_stat(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          fmt::format("output_size[{}]", func_id))),
      queueing_delay_stat(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("queueing_delay[{}]", func_id))),
      running_delay_stat(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("running_delay[{}]", func_id))),
      inflight_requests_stat(stat::StatisticsCollector<uint16_t>::StandardReportCallback(
          fmt::format("inflight_requests[{}]", func_id))),
      instant_rps_ema(absl::GetFlag(FLAGS_instant_rps_ema_tau_ms),
                      absl::GetFlag(FLAGS_instant_rps_ema_alpha),
                      absl::GetFlag(FLAGS_instant_rps_p_norm)),
      running_delay_ema(/* alpha= */ 0.001),
      processing_time_ema(/* alpha= */ 0.001),
      processing_time2_ema(/* alpha= */ 0.001) {}

}  // namespace engine
}  // namespace faas
