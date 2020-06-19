#include "gateway/dispatcher.h"

#include "ipc/base.h"
#include "gateway/server.h"

#include <absl/flags/flag.h>

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

using protocol::FuncCall;
using protocol::FuncCallDebugString;
using protocol::Message;
using protocol::SetInlineDataInMessage;
using protocol::GetFuncCallFromMessage;
using protocol::NewCreateFuncWorkerMessage;
using protocol::NewDispatchFuncCallMessage;

Dispatcher::Dispatcher(Server* server, uint16_t func_id)
    : server_(server), func_id_(func_id),
      log_header_(fmt::format("Dispatcher[{}]: ", func_id)),
      idle_workers_stat_(stat::StatisticsCollector<uint16_t>::StandardReportCallback(
          fmt::format("idle_workers[{}]", func_id))),
      running_workers_stat_(stat::StatisticsCollector<uint16_t>::StandardReportCallback(
          fmt::format("running_workers[{}]", func_id))) {}

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

bool Dispatcher::OnNewFuncCall(const protocol::FuncCall& func_call,
                               const protocol::FuncCall& parent_func_call,
                               size_t input_size, std::span<const char> inline_input,
                               bool shm_input) {
    VLOG(1) << "OnNewFuncCall " << FuncCallDebugString(func_call);
    DCHECK_EQ(func_id_, func_call.func_id);
    absl::MutexLock lk(&mu_);
    Message* dispatch_func_call_message = message_pool_.Get();
    if (shm_input) {
        dispatch_func_call_message->payload_size = -gsl::narrow_cast<int32_t>(input_size);
    } else {
        SetInlineDataInMessage(dispatch_func_call_message, inline_input);
    }

    Tracer::FuncCallInfo* func_call_info = server_->tracer()->OnNewFuncCall(
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

void Dispatcher::OnFuncCallCompleted(const FuncCall& func_call,
                                     int32_t processing_time, size_t output_size) {
    VLOG(1) << "OnFuncCallCompleted " << FuncCallDebugString(func_call);
    DCHECK_EQ(func_id_, func_call.func_id);
    absl::MutexLock lk(&mu_);
    server_->tracer()->OnFuncCallCompleted(func_call, processing_time, output_size);
    server_->tracer()->DiscardFuncCallInfo(func_call);
    if (!assigned_workers_.contains(func_call.full_call_id)) {
        HLOG(ERROR) << "Cannot find assigned worker for full_call_id " << func_call.full_call_id;
        return;
    }
    FuncWorker* func_worker = assigned_workers_[func_call.full_call_id];
    FuncWorkerFinished(func_worker);
    assigned_workers_.erase(func_call.full_call_id);
}

void Dispatcher::OnFuncCallFailed(const FuncCall& func_call) {
    VLOG(1) << "OnFuncCallFailed " << FuncCallDebugString(func_call);
    DCHECK_EQ(func_id_, func_call.func_id);
    absl::MutexLock lk(&mu_);
    server_->tracer()->OnFuncCallFailed(func_call);
    server_->tracer()->DiscardFuncCallInfo(func_call);
    if (!assigned_workers_.contains(func_call.full_call_id)) {
        return;
    }
    FuncWorker* func_worker = assigned_workers_[func_call.full_call_id];
    FuncWorkerFinished(func_worker);
    assigned_workers_.erase(func_call.full_call_id);
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
    PendingFuncCall pending_func_call = pending_func_calls_.front();
    pending_func_calls_.pop();
    Message* dispatch_func_call_message = pending_func_call.dispatch_func_call_message;
    FuncCall func_call = GetFuncCallFromMessage(*dispatch_func_call_message);
    HVLOG(1) << "Found pending func_call " << FuncCallDebugString(func_call);
    DispatchFuncCall(func_worker, dispatch_func_call_message);
    return true;
}

void Dispatcher::DispatchFuncCall(FuncWorker* func_worker, Message* dispatch_func_call_message) {
    uint16_t client_id = func_worker->client_id();
    DCHECK(workers_.contains(client_id));
    DCHECK(!running_workers_.contains(client_id));
    FuncCall func_call = GetFuncCallFromMessage(*dispatch_func_call_message);
    server_->tracer()->OnFuncCallDispatched(func_call, func_worker);
    assigned_workers_[func_call.full_call_id] = func_worker;
    running_workers_[client_id] = func_call;
    func_worker->DispatchFuncCall(dispatch_func_call_message);
    message_pool_.Return(dispatch_func_call_message);
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
