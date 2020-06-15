#include "gateway/dispatcher.h"

#include "ipc/base.h"
#include "ipc/fifo.h"
#include "gateway/server.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

using protocol::FuncCall;
using protocol::FuncCallDebugString;
using protocol::Message;
using protocol::SetInlineDataInMessage;
using protocol::GetFuncCallFromMessage;
using protocol::SetProfilingFieldsInMessage;
using protocol::NewCreateFuncWorkerMessage;
using protocol::NewInvokeFuncMessage;

Dispatcher::Dispatcher(Server* server, uint16_t func_id)
    : server_(server), func_id_(func_id),
      log_header_(fmt::format("Dispatcher[{}]: ", func_id)),
      incoming_requests_stat_(stat::Counter::StandardReportCallback(
          fmt::format("incoming_requests[{}]", func_id))),
      failed_requests_stat_(stat::Counter::StandardReportCallback(
          fmt::format("failed_requests[{}]", func_id))),
      input_size_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          fmt::format("input_size[{}]", func_id))),
      output_size_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          fmt::format("output_size[{}]", func_id))),
      queueing_delay_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("queueing_delay[{}]", func_id))),
      processing_delay_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("processing_delay[{}]", func_id))),
      dispatch_overhead_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("dispatch_overhead[{}]", func_id))),
      idle_workers_stat_(stat::StatisticsCollector<uint16_t>::StandardReportCallback(
          fmt::format("idle_workers[{}]", func_id))),
      running_workers_stat_(stat::StatisticsCollector<uint16_t>::StandardReportCallback(
          fmt::format("running_workers[{}]", func_id))),
      inflight_requests_stat_(stat::StatisticsCollector<uint16_t>::StandardReportCallback(
          fmt::format("inflight_requests[{}]", func_id))) {}

Dispatcher::~Dispatcher() {}

bool Dispatcher::OnFuncWorkerConnected(FuncWorker* func_worker) {
    DCHECK_EQ(func_id_, func_worker->func_id());
    uint16_t client_id = func_worker->client_id();
    absl::MutexLock lk(&mu_);
    DCHECK(!workers_.contains(client_id));
    workers_[client_id] = func_worker;
    if (!DispatchPendingFuncCall(func_worker)) {
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

bool Dispatcher::OnNewFuncCall(const protocol::FuncCall& func_call, size_t input_size,
                               std::span<const char> inline_input, bool shm_input) {
    VLOG(1) << "OnNewFuncCall " << FuncCallDebugString(func_call);
    DCHECK_EQ(func_id_, func_call.func_id);
    Message invoke_func_message = NewInvokeFuncMessage(func_call);
    if (shm_input) {
        invoke_func_message.payload_size = -gsl::narrow_cast<int32_t>(input_size);
    } else {
        SetInlineDataInMessage(&invoke_func_message, inline_input);
    }
    absl::MutexLock lk(&mu_);
    func_call_states_[func_call.full_call_id] = {
        .recv_timestamp = GetMonotonicMicroTimestamp(),
        .dispatch_timestamp = 0,
        .assigned_worker = nullptr
    };
    inflight_requests_stat_.AddSample(gsl::narrow_cast<uint16_t>(func_call_states_.size()));
    incoming_requests_stat_.Tick();
    input_size_stat_.AddSample(gsl::narrow_cast<uint32_t>(input_size));
    FuncWorker* idle_worker = PickIdleWorker();
    if (idle_worker) {
        DispatchFuncCall(idle_worker, &invoke_func_message);
    } else {
        VLOG(1) << "No idle worker at the moment";
        pending_func_calls_.push(invoke_func_message);
    }
    return true;
}

void Dispatcher::OnFuncCallCompleted(const FuncCall& func_call,
                                     int32_t processing_time, size_t output_size) {
    VLOG(1) << "OnFuncCallCompleted " << FuncCallDebugString(func_call);
    DCHECK_EQ(func_id_, func_call.func_id);
    absl::MutexLock lk(&mu_);
    if (!func_call_states_.contains(func_call.full_call_id)) {
        HLOG(ERROR) << "Cannot find FuncCallState for full_call_id " << func_call.full_call_id;
        return;
    }
    FuncCallState* func_call_state = &func_call_states_[func_call.full_call_id];
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    processing_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(
        current_timestamp - func_call_state->dispatch_timestamp));
    if (processing_time > 0) {
        dispatch_overhead_stat_.AddSample(gsl::narrow_cast<int32_t>(
            current_timestamp - func_call_state->dispatch_timestamp - processing_time));
    } else {
        HLOG(WARNING) << "processing_time is not set";
    }
    output_size_stat_.AddSample(gsl::narrow_cast<uint32_t>(output_size));
    FuncWorker* func_worker = func_call_state->assigned_worker;
    if (func_worker != nullptr) {
        FuncWorkerFinished(func_worker);
    } else {
        HLOG(ERROR) << "There is no assigned FuncWorker for full_call_id "
                    << func_call.full_call_id;
    }
    func_call_states_.erase(func_call.full_call_id);
}

void Dispatcher::OnFuncCallFailed(const FuncCall& func_call) {
    VLOG(1) << "OnFuncCallFailed " << FuncCallDebugString(func_call);
    DCHECK_EQ(func_id_, func_call.func_id);
    absl::MutexLock lk(&mu_);
    if (!func_call_states_.contains(func_call.full_call_id)) {
        HLOG(ERROR) << "Cannot find FuncCallState for full_call_id " << func_call.full_call_id;
        return;
    }
    failed_requests_stat_.Tick();
    FuncCallState* func_call_state = &func_call_states_[func_call.full_call_id];
    FuncWorker* func_worker = func_call_state->assigned_worker;
    if (func_worker != nullptr) {
        FuncWorkerFinished(func_worker);
    } else {
        HLOG(ERROR) << "There is no assigned FuncWorker for full_call_id "
                    << func_call.full_call_id;
    }
    func_call_states_.erase(func_call.full_call_id);
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
    Message* invoke_func_message = &pending_func_calls_.front();
    FuncCall func_call = GetFuncCallFromMessage(*invoke_func_message);
    HVLOG(1) << "Found pending func_call " << FuncCallDebugString(func_call);
    DispatchFuncCall(func_worker, invoke_func_message);
    pending_func_calls_.pop();
    return true;
}

void Dispatcher::DispatchFuncCall(FuncWorker* func_worker, Message* invoke_func_message) {
    uint16_t client_id = func_worker->client_id();
    DCHECK(workers_.contains(client_id));
    DCHECK(!running_workers_.contains(client_id));
    FuncCall func_call = GetFuncCallFromMessage(*invoke_func_message);
    running_workers_[client_id] = func_call;
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    DCHECK(func_call_states_.contains(func_call.full_call_id));
    FuncCallState* func_call_state = &func_call_states_[func_call.full_call_id];
    func_call_state->dispatch_timestamp = current_timestamp;
    func_call_state->assigned_worker = func_worker;
    queueing_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(
        current_timestamp - func_call_state->recv_timestamp));
    func_worker->DispatchFuncCall(invoke_func_message);
}

FuncWorker* Dispatcher::PickIdleWorker() {
    while (!idle_workers_.empty()) {
        uint16_t client_id = idle_workers_.back();
        idle_workers_.pop_back();
        if (workers_.contains(client_id) && !running_workers_.contains(client_id)) {
            return workers_[client_id];
        }
    }
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

}  // namespace gateway
}  // namespace faas
