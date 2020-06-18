#include "gateway/tracer.h"

#include "gateway/server.h"
#include "gateway/worker_manager.h"

#define HLOG(l) LOG(l) << "Tracer: "
#define HVLOG(l) VLOG(l) << "Tracer: "

namespace faas {
namespace gateway {

using protocol::FuncCall;
using protocol::FuncCallDebugString;

Tracer::Tracer(Server* server)
    : server_(server),
      dispatch_overhead_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("dispatch_overhead")) {
    for (int i = 0; i < protocol::kMaxFuncId; i++) {
        per_func_states_[i] = nullptr;
    }
}

Tracer::~Tracer() {
    for (int i = 0; i < protocol::kMaxFuncId; i++) {
        if (per_func_states_[i] != nullptr) {
            delete per_func_states_[i];
        }
    }
}

void Tracer::Init() {
    for (int i = 0; i < protocol::kMaxFuncId; i++) {
        if (server_->func_config()->find_by_func_id(i) != nullptr) {
            per_func_states_[i] = new PerFuncState(i);
        }
    }
}

void Tracer::OnNewFuncCall(const FuncCall& func_call, const FuncCall& parent_func_call,
                           size_t input_size) {
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    FuncCallInfo* info;

    {
        absl::MutexLock lk(&mu_);
        if (func_call_infos_.contains(func_call.full_call_id)) {
            HLOG(WARNING) << "FuncCall already exists: " << FuncCallDebugString(func_call);
            return;
        }
        info = func_call_info_pool_.Get();
        info->reset();
        func_call_infos_[func_call.full_call_id] = info;
    }

    {
        FuncCallInfoGuard guard1(info);
        info->state = kReceived;
        info->func_call = func_call;
        info->parent_func_call = parent_func_call;
        info->input_size = input_size;
        info->recv_timestamp = current_timestamp;

        uint16_t func_id = func_call.func_id;
        DCHECK_LT(func_id, protocol::kMaxFuncId);
        DCHECK(per_func_states_[func_id] != nullptr);
        PerFuncState* per_func_state = per_func_states_[func_id];
        PerFuncStateGuard guard2(per_func_state);

        if (per_func_state->last_request_timestamp != -1) {
            double instant_rps = gsl::narrow_cast<double>(
                1e6 / (current_timestamp - per_func_state->last_request_timestamp));
            per_func_state->instant_rps_stat.AddSample(instant_rps);
            per_func_state->rps_ema.AddSample(instant_rps);
        }
        per_func_state->last_request_timestamp = current_timestamp;
        per_func_state->inflight_requests++;
        per_func_state->inflight_requests_stat.AddSample(
            gsl::narrow_cast<uint16_t>(per_func_state->inflight_requests));
        per_func_state->incoming_requests_stat.Tick();
        per_func_state->input_size_stat.AddSample(gsl::narrow_cast<uint32_t>(input_size));
    }
}

void Tracer::OnFuncCallDispatched(const protocol::FuncCall& func_call, FuncWorker* func_worker) {
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    FuncCallInfo* info;

    {
        absl::MutexLock lk(&mu_);
        if (!func_call_infos_.contains(func_call.full_call_id)) {
            HLOG(WARNING) << "Cannot find FuncCall: " << FuncCallDebugString(func_call);
            return;
        }
        info = func_call_infos_[func_call.full_call_id];
    }

    {
        FuncCallInfoGuard guard1(info);
        info->state = kDispatched;
        info->dispatch_timestamp = current_timestamp;
        info->assigned_worker = func_worker;

        PerFuncState* per_func_state = per_func_states_[func_call.func_id];
        PerFuncStateGuard guard2(per_func_state);
        per_func_state->queueing_delay_stat.AddSample(
            gsl::narrow_cast<int32_t>(current_timestamp - info->recv_timestamp));
    }
}

void Tracer::OnFuncCallCompleted(const FuncCall& func_call, int32_t processing_time,
                                 size_t output_size) {
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    FuncCallInfo* info;

    {
        absl::MutexLock lk(&mu_);
        if (!func_call_infos_.contains(func_call.full_call_id)) {
            HLOG(WARNING) << "Cannot find FuncCall: " << FuncCallDebugString(func_call);
            return;
        }
        info = func_call_infos_[func_call.full_call_id];
        dispatch_overhead_stat_.AddSample(gsl::narrow_cast<int32_t>(
            current_timestamp - info->dispatch_timestamp - processing_time));
    }

    {
        FuncCallInfoGuard guard1(info);
        info->state = kCompleted;
        info->output_size = output_size;
        info->finish_timestamp = current_timestamp;
        info->processing_time = processing_time;

        PerFuncState* per_func_state = per_func_states_[func_call.func_id];
        PerFuncStateGuard guard2(per_func_state);

        int32_t running_delay = gsl::narrow_cast<int32_t>(current_timestamp - info->dispatch_timestamp);
        per_func_state->running_delay_stat.AddSample(running_delay);
        per_func_state->inflight_requests--;
        if (per_func_state->inflight_requests < 0) {
            HLOG(ERROR) << "Negative inflight_requests for func_id " << per_func_state->func_id;
            per_func_state->inflight_requests = 0;
        }
        per_func_state->output_size_stat.AddSample(gsl::narrow_cast<uint32_t>(output_size));
        per_func_state->running_delay_ema.AddSample(running_delay);
        per_func_state->processing_time_ema.AddSample(processing_time);
    }

    {
        absl::MutexLock lk(&mu_);
        func_call_infos_.erase(func_call.full_call_id);
        func_call_info_pool_.Return(info);
    }
}

void Tracer::OnFuncCallFailed(const FuncCall& func_call) {
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    FuncCallInfo* info;

    {
        absl::MutexLock lk(&mu_);
        if (!func_call_infos_.contains(func_call.full_call_id)) {
            HLOG(WARNING) << "Cannot find FuncCall: " << FuncCallDebugString(func_call);
            return;
        }
        info = func_call_infos_[func_call.full_call_id];
    }

    {
        FuncCallInfoGuard guard1(info);
        info->state = kFailed;
        info->finish_timestamp = current_timestamp;

        PerFuncState* per_func_state = per_func_states_[func_call.func_id];
        PerFuncStateGuard guard2(per_func_state);

        per_func_state->failed_requests_stat.Tick();
        per_func_state->inflight_requests--;
        if (per_func_state->inflight_requests < 0) {
            HLOG(ERROR) << "Negative inflight_requests for func_id " << per_func_state->func_id;
            per_func_state->inflight_requests = 0;
        }
    }

    {
        absl::MutexLock lk(&mu_);
        func_call_infos_.erase(func_call.full_call_id);
        func_call_info_pool_.Return(info);
    }
}

Tracer::PerFuncState::PerFuncState(uint16_t func_id)
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
      rps_ema(/* alpha= */ 0.999, /* p= */ 1.0),
      running_delay_ema(/* alpha= */ 0.999, /* p= */ 1.0),
      processing_time_ema(/* alpha= */ 0.999, /* p= */ 1.0),
      in_use(0) {}

Tracer::FuncCallInfoGuard::FuncCallInfoGuard(FuncCallInfo* func_call_info)
    : func_call_info_(func_call_info) {
#if DCHECK_IS_ON()
    int zero = 0;
    if (!func_call_info_->in_use.compare_exchange_strong(zero, 1)) {
        HLOG(FATAL) << fmt::format("FuncCallInfo of {} is in use",
                                   FuncCallDebugString(func_call_info->func_call));
    }
#endif
}

Tracer::FuncCallInfoGuard::~FuncCallInfoGuard() {
#if DCHECK_IS_ON()
    int one = 1;
    CHECK(func_call_info_->in_use.compare_exchange_strong(one, 0));
#endif
}

Tracer::PerFuncStateGuard::PerFuncStateGuard(PerFuncState* per_func_state)
    : per_func_state_(per_func_state) {
#if DCHECK_IS_ON()
    int zero = 0;
    if (!per_func_state_->in_use.compare_exchange_strong(zero, 1)) {
        HLOG(FATAL) << fmt::format("PerFuncState of func_id {} is in use", per_func_state_->func_id);
    }
#endif
}

Tracer::PerFuncStateGuard::~PerFuncStateGuard() {
#if DCHECK_IS_ON()
    int one = 1;
    CHECK(per_func_state_->in_use.compare_exchange_strong(one, 0));
#endif
}

}  // namespace gateway
}  // namespace faas
