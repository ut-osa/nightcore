#include "engine/dispatcher.h"

#include "ipc/base.h"
#include "engine/engine.h"

#include <absl/flags/flag.h>

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

ABSL_FLAG(double, max_relative_queueing_delay, 0.0, "");
ABSL_FLAG(double, concurrency_limit_coef, 1.0, "");
ABSL_FLAG(double, expected_concurrency_coef, 1.0, "");
ABSL_FLAG(int, min_worker_request_interval_ms, 200, "");

namespace faas {
namespace engine {

using protocol::FuncCall;
using protocol::FuncCallDebugString;
using protocol::Message;
using protocol::SetInlineDataInMessage;
using protocol::GetFuncCallFromMessage;
using protocol::NewCreateFuncWorkerMessage;
using protocol::NewDispatchFuncCallMessage;

Dispatcher::Dispatcher(Engine* engine, uint16_t func_id)
    : engine_(engine), func_id_(func_id),
      log_header_(fmt::format("Dispatcher[{}]: ", func_id)),
      last_request_worker_timestamp_(-1),
      idle_workers_stat_(stat::StatisticsCollector<uint16_t>::StandardReportCallback(
          fmt::format("idle_workers[{}]", func_id))),
      running_workers_stat_(stat::StatisticsCollector<uint16_t>::StandardReportCallback(
          fmt::format("running_workers[{}]", func_id))),
      max_concurrency_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          fmt::format("max_concurrency[{}]", func_id))),
      estimated_concurrency_stat_(stat::StatisticsCollector<float>::StandardReportCallback(
          fmt::format("estimated_concurrency[{}]", func_id))) {
    const FuncConfig::Entry* func_entry = engine_->func_config()->find_by_func_id(func_id);
    DCHECK(func_entry != nullptr);
    func_config_entry_ = func_entry;
    if (func_config_entry_->min_workers > 0) {
        HLOG(INFO) << "min_workers=" << func_config_entry_->min_workers;
    }
    if (func_config_entry_->max_workers > 0) {
        HLOG(INFO) << "max_workers=" << func_config_entry_->max_workers;
    }
}

Dispatcher::~Dispatcher() {}

bool Dispatcher::OnFuncWorkerConnected(std::shared_ptr<FuncWorker> func_worker) {
    DCHECK_EQ(func_id_, func_worker->func_id());
    uint16_t client_id = func_worker->client_id();
    absl::MutexLock lk(&mu_);
    DCHECK(!workers_.contains(client_id));
    workers_[client_id] = func_worker;
    if (requested_workers_.contains(client_id)) {
        int64_t request_timestamp = requested_workers_[client_id];
        requested_workers_.erase(client_id);
        HLOG(INFO) << fmt::format("FuncWorker (client_id {}) takes {}ms to launch",
                                  client_id,
                                  (GetMonotonicMicroTimestamp() - request_timestamp) / 1000);
    }
    if (!DispatchPendingFuncCall(func_worker.get())) {
        idle_workers_.push_back(client_id);
    }
    UpdateWorkerLoadStat();
    return true;
}

void Dispatcher::OnFuncWorkerDisconnected(FuncWorker* func_worker) {
    DCHECK_EQ(func_id_, func_worker->func_id());
    uint16_t client_id = func_worker->client_id();
    absl::MutexLock lk(&mu_);
    DCHECK(workers_.contains(client_id));
    workers_.erase(client_id);
}

bool Dispatcher::OnNewFuncCall(const FuncCall& func_call, const FuncCall& parent_func_call,
                               size_t input_size, std::span<const char> inline_input,
                               bool shm_input) {
    VLOG(1) << "OnNewFuncCall " << FuncCallDebugString(func_call);
    DCHECK_EQ(func_id_, func_call.func_id);
    absl::MutexLock lk(&mu_);
    Message* dispatch_func_call_message = message_pool_.Get();
    *dispatch_func_call_message = NewDispatchFuncCallMessage(func_call);
    if (shm_input) {
        dispatch_func_call_message->payload_size = -gsl::narrow_cast<int32_t>(input_size);
    } else {
        SetInlineDataInMessage(dispatch_func_call_message, inline_input);
    }

    Tracer::FuncCallInfo* func_call_info = engine_->tracer()->OnNewFuncCall(
        func_call, parent_func_call, input_size);
    FuncWorker* idle_worker = PickIdleWorker();
    if (idle_worker) {
        DispatchFuncCall(idle_worker, dispatch_func_call_message);
    } else {
        VLOG(1) << "No idle worker at the moment";
        pending_func_calls_.push({
            .dispatch_func_call_message = dispatch_func_call_message,
            .func_call_info = func_call_info
        });
    }
    return true;
}

bool Dispatcher::OnFuncCallCompleted(const FuncCall& func_call, int32_t processing_time,
                                     int32_t dispatch_delay, size_t output_size) {
    VLOG(1) << "OnFuncCallCompleted " << FuncCallDebugString(func_call);
    DCHECK_EQ(func_id_, func_call.func_id);
    Tracer::FuncCallInfo* func_call_info = engine_->tracer()->OnFuncCallCompleted(
        func_call, dispatch_delay, processing_time, output_size);
    if (func_call_info == nullptr) {
        return false;
    }
    engine_->tracer()->DiscardFuncCallInfo(func_call);
    absl::MutexLock lk(&mu_);
    if (!assigned_workers_.contains(func_call.full_call_id)) {
        return true;
    }
    uint16_t client_id = assigned_workers_[func_call.full_call_id];
    if (workers_.contains(client_id)) {
        FuncWorker* func_worker = workers_[client_id].get();
        FuncWorkerFinished(func_worker);
    } else {
        HLOG(WARNING) << fmt::format("FuncWorker (client_id {}) already disconnected", client_id);
    }
    assigned_workers_.erase(func_call.full_call_id);
    return true;
}

bool Dispatcher::OnFuncCallFailed(const FuncCall& func_call, int32_t dispatch_delay) {
    VLOG(1) << "OnFuncCallFailed " << FuncCallDebugString(func_call);
    DCHECK_EQ(func_id_, func_call.func_id);
    Tracer::FuncCallInfo* func_call_info = engine_->tracer()->OnFuncCallFailed(
        func_call, dispatch_delay);
    if (func_call_info == nullptr) {
        return false;
    }
    engine_->tracer()->DiscardFuncCallInfo(func_call);
    absl::MutexLock lk(&mu_);
    if (!assigned_workers_.contains(func_call.full_call_id)) {
        return true;
    }
    uint16_t client_id = assigned_workers_[func_call.full_call_id];
    if (workers_.contains(client_id)) {
        FuncWorker* func_worker = workers_[client_id].get();
        FuncWorkerFinished(func_worker);
    } else {
        HLOG(WARNING) << fmt::format("FuncWorker (client_id {}) already disconnected", client_id);
    }
    assigned_workers_.erase(func_call.full_call_id);
    return true;
}

void Dispatcher::FuncWorkerFinished(FuncWorker* func_worker) {
    uint16_t client_id = func_worker->client_id();
    DCHECK(workers_.contains(client_id));
    DCHECK(running_workers_.contains(client_id));
    running_workers_.erase(client_id);
    if (!DispatchPendingFuncCall(func_worker)) {
        idle_workers_.push_back(client_id);
    }
    UpdateWorkerLoadStat();
}

bool Dispatcher::DispatchPendingFuncCall(FuncWorker* func_worker) {
    if (pending_func_calls_.empty()) {
        return false;
    }
    double average_processing_time = engine_->tracer()->GetAverageProcessingTime(func_id_);
    double max_relative_queueing_delay = absl::GetFlag(FLAGS_max_relative_queueing_delay);
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    while (!pending_func_calls_.empty()) {
        PendingFuncCall pending_func_call = pending_func_calls_.front();
        pending_func_calls_.pop();
        Tracer::FuncCallInfo* func_call_info = pending_func_call.func_call_info;
        int64_t queueing_delay;
        {
            absl::ReaderMutexLock lk(&func_call_info->mu);
            queueing_delay = current_timestamp - func_call_info->recv_timestamp;
        }
        Message* dispatch_func_call_message = pending_func_call.dispatch_func_call_message;
        FuncCall func_call = GetFuncCallFromMessage(*dispatch_func_call_message);
        if (func_call.client_id == 0
                || max_relative_queueing_delay == 0.0
                || queueing_delay <= max_relative_queueing_delay * average_processing_time) {
            DispatchFuncCall(func_worker, dispatch_func_call_message);
            return true;
        } else {
            message_pool_.Return(dispatch_func_call_message);
            engine_->DiscardFuncCall(func_call);
            engine_->tracer()->DiscardFuncCallInfo(func_call);
        }
    }
    return false;
}

void Dispatcher::DispatchFuncCall(FuncWorker* func_worker, Message* dispatch_func_call_message) {
    uint16_t client_id = func_worker->client_id();
    DCHECK(workers_.contains(client_id));
    DCHECK(!running_workers_.contains(client_id));
    FuncCall func_call = GetFuncCallFromMessage(*dispatch_func_call_message);
    engine_->tracer()->OnFuncCallDispatched(func_call, func_worker);
    assigned_workers_[func_call.full_call_id] = client_id;
    running_workers_[client_id] = func_call;
    func_worker->DispatchFuncCall(dispatch_func_call_message);
    message_pool_.Return(dispatch_func_call_message);
}

FuncWorker* Dispatcher::PickIdleWorker() {
    size_t max_concurrency = DetermineConcurrencyLimit();
    max_concurrency_stat_.AddSample(gsl::narrow_cast<uint32_t>(max_concurrency));
    if (running_workers_.size() >= max_concurrency) {
        return nullptr;
    }
    while (!idle_workers_.empty()) {
        uint16_t client_id = idle_workers_.back();
        idle_workers_.pop_back();
        if (workers_.contains(client_id) && !running_workers_.contains(client_id)) {
            return workers_[client_id].get();
        }
    }
    MayRequestNewFuncWorker();
    return nullptr;
}

void Dispatcher::UpdateWorkerLoadStat() {
    size_t total_workers = workers_.size();
    size_t running_workers = running_workers_.size();
    size_t idle_workers = total_workers - running_workers;
    HVLOG(1) << fmt::format("UpdateWorkerLoadStat: running_workers={}, idle_workers={}",
                            running_workers, idle_workers);
    idle_workers_stat_.AddSample(gsl::narrow_cast<uint16_t>(idle_workers));
    running_workers_stat_.AddSample(gsl::narrow_cast<uint16_t>(running_workers));
}

size_t Dispatcher::DetermineExpectedConcurrency() {
    double average_processing_time = engine_->tracer()->GetAverageProcessingTime2(func_id_);
    double average_instant_rps = engine_->tracer()->GetAverageInstantRps(func_id_);
    if (average_processing_time > 0 && average_instant_rps > 0) {
        double estimated_concurrency = absl::GetFlag(FLAGS_expected_concurrency_coef)
                                     * average_processing_time * average_instant_rps / 1e6;
        return gsl::narrow_cast<size_t>(0.5 + estimated_concurrency);
    } else {
        return 0;
    }
}

size_t Dispatcher::DetermineConcurrencyLimit() {
    size_t result = std::numeric_limits<size_t>::max();
    double average_running_delay = engine_->tracer()->GetAverageRunningDelay(func_id_);
    double average_instant_rps = engine_->tracer()->GetAverageInstantRps(func_id_);
    if (average_running_delay > 0 && average_instant_rps > 0) {
        double estimated_concurrency = absl::GetFlag(FLAGS_concurrency_limit_coef)
                                     * average_running_delay * average_instant_rps / 1e6;
        estimated_concurrency_stat_.AddSample(gsl::narrow_cast<float>(estimated_concurrency));
        result = gsl::narrow_cast<size_t>(0.5 + estimated_concurrency);
    }
    if (func_config_entry_->max_workers > 0) {
        result = std::min(result, gsl::narrow_cast<size_t>(func_config_entry_->max_workers));
    }
    if (func_config_entry_->min_workers > 0) {
        result = std::max(result, gsl::narrow_cast<size_t>(func_config_entry_->min_workers));
    }
    return result;
}

void Dispatcher::MayRequestNewFuncWorker() {
    size_t estimated_concurrency = DetermineExpectedConcurrency();
    size_t max_workers = std::numeric_limits<size_t>::max();
    if (func_config_entry_->max_workers > 0) {
        max_workers = gsl::narrow_cast<size_t>(func_config_entry_->max_workers);
    }
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    int min_worker_request_interval_ms = absl::GetFlag(FLAGS_min_worker_request_interval_ms);
    if (estimated_concurrency > 0
            && workers_.size() + requested_workers_.size() < max_workers
            && workers_.size() + requested_workers_.size() < estimated_concurrency
            && (last_request_worker_timestamp_ == -1
                || current_timestamp > last_request_worker_timestamp_
                                       + min_worker_request_interval_ms * 1000)) {
        HLOG(INFO) << "Request new FuncWorker: estimated_concurrency=" << estimated_concurrency;
        uint16_t client_id;
        if (engine_->worker_manager()->RequestNewFuncWorker(func_id_, &client_id)) {
            requested_workers_[client_id] = current_timestamp;
            last_request_worker_timestamp_ = current_timestamp;
        } else {
            HLOG(ERROR) << "Failed to request new FuncWorker";
        }
    }
}

}  // namespace engine
}  // namespace faas
