#include "gateway/dispatcher.h"

#include "ipc/base.h"
#include "ipc/fifo.h"
#include "gateway/server.h"

#define HLOG(l) LOG(l) << "Dispatcher: "
#define HVLOG(l) VLOG(l) << "Dispatcher: "

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

constexpr int Dispatcher::kDefaultMinWorkersPerFunc;
constexpr int Dispatcher::kMaxClientId;

Dispatcher::Dispatcher(Server* server)
    : server_(server), next_client_id_(1) {}

Dispatcher::~Dispatcher() {}

bool Dispatcher::OnLauncherConnected(MessageConnection* launcher_connection) {
    uint16_t func_id = launcher_connection->func_id();
    HLOG(INFO) << "Launcher of func_id " << func_id << " connected";
    absl::MutexLock lk(&mu_);
    if (launcher_connections_.contains(func_id)) {
        HLOG(ERROR) << "Launcher of func_id " << func_id << " already connected";
        return false;
    }
    launcher_connections_[func_id] = launcher_connection;
    per_func_states_[func_id] = std::make_unique<PerFuncState>(func_id);
    const FuncConfig::Entry* func_entry = server_->func_config()->find_by_func_id(func_id);
    DCHECK(func_entry != nullptr);
    int min_workers = func_entry->min_workers;
    if (min_workers == -1) {
        min_workers = kDefaultMinWorkersPerFunc;
    }
    for (int i = 0; i < min_workers; i++) {
        RequestNewFuncWorker(launcher_connection);
    }
    return true;
}

bool Dispatcher::OnFuncWorkerConnected(MessageConnection* worker_connection) {
    uint16_t func_id = worker_connection->func_id();
    uint16_t client_id = worker_connection->client_id();
    HLOG(INFO) << "FuncWorker of func_id " << func_id
               << ", client_id " << client_id << " connected";
    absl::MutexLock lk(&mu_);
    if (!per_func_states_.contains(func_id)) {
        HLOG(ERROR) << "PerFuncState of func_id " << func_id << " does not exist";
        return false;
    }
    auto per_func_state = per_func_states_[func_id].get();
    per_func_state->NewWorker(worker_connection);
    DispatchPendingFuncCall(worker_connection, per_func_state);
    return true;
}

void Dispatcher::OnLauncherDisconnected(MessageConnection* launcher_connection) {
    uint16_t func_id = launcher_connection->func_id();
    HLOG(INFO) << "Launcher of func_id " << func_id << " disconnected";
    absl::MutexLock lk(&mu_);
    if (launcher_connections_.contains(func_id)
          && launcher_connections_[func_id] == launcher_connection) {
        launcher_connections_.erase(func_id);
    } else {
        HLOG(ERROR) << "Cannot find launcher connection for func_id " << func_id;
    }
}

void Dispatcher::OnFuncWorkerDisconnected(MessageConnection* worker_connection) {
    uint16_t func_id = worker_connection->func_id();
    uint16_t client_id = worker_connection->client_id();
    HLOG(INFO) << "FuncWorker of func_id " << func_id
               << ", client_id " << client_id << " disconnected";
    absl::MutexLock lk(&mu_);
    DCHECK(per_func_states_.contains(func_id));
    per_func_states_[func_id]->RemoveWorker(worker_connection);
    ipc::FifoRemove(ipc::GetFuncWorkerInputFifoName(client_id));
    ipc::FifoRemove(ipc::GetFuncWorkerOutputFifoName(client_id));
}

bool Dispatcher::OnNewFuncCall(MessageConnection* caller_connection,
                               const protocol::FuncCall& func_call, size_t input_size,
                               std::span<const char> inline_input, bool shm_input) {
    VLOG(1) << "OnNewFuncCall " << FuncCallDebugString(func_call);
    uint16_t func_id = func_call.func_id;
    absl::MutexLock lk(&mu_);
    if (!launcher_connections_.contains(func_id)) {
        HLOG(ERROR) << "There is no launcher for func_id " << func_id;
        return false;
    }
    per_func_call_states_[func_call.full_call_id] = {
        .recv_timestamp = GetMonotonicMicroTimestamp(),
        .dispatch_timestamp = 0
    };
    DCHECK(per_func_states_.contains(func_id));
    auto per_func_state = per_func_states_[func_id].get();
    per_func_state->incoming_requests_stat()->Tick();
    per_func_state->input_size_stat()->AddSample(gsl::narrow_cast<uint32_t>(input_size));
    Message invoke_func_message = NewInvokeFuncMessage(func_call);
    if (shm_input) {
        invoke_func_message.payload_size = -gsl::narrow_cast<int32_t>(input_size);
    } else {
        SetInlineDataInMessage(&invoke_func_message, inline_input);
    }
    MessageConnection* idle_worker = per_func_state->PickIdleWorker();
    if (idle_worker) {
        DispatchFuncCall(idle_worker, &invoke_func_message);
        per_func_state->WorkerBecomeRunning(idle_worker, func_call);
    } else {
        VLOG(1) << "No idle worker at the moment";
        per_func_state->PushPendingFuncCall(invoke_func_message);
    }
    return true;
}

void Dispatcher::OnFuncCallCompleted(MessageConnection* worker_connection, const FuncCall& func_call,
                                     int32_t processing_time, size_t output_size) {
    VLOG(1) << "OnFuncCallCompleted " << FuncCallDebugString(func_call);
    uint16_t func_id = func_call.func_id;
    DCHECK_EQ(func_id, worker_connection->func_id());
    absl::MutexLock lk(&mu_);
    DCHECK(per_func_call_states_.contains(func_call.full_call_id));
    auto per_func_call_state = &per_func_call_states_[func_call.full_call_id];
    DCHECK(per_func_states_.contains(func_id));
    auto per_func_state = per_func_states_[func_id].get();
    FuncWorkerFinished(per_func_state, worker_connection);
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    per_func_state->processing_delay_stat()->AddSample(gsl::narrow_cast<int32_t>(
        current_timestamp - per_func_call_state->dispatch_timestamp));
    if (processing_time > 0) {
        per_func_state->dispatch_overhead_stat()->AddSample(gsl::narrow_cast<int32_t>(
            current_timestamp - per_func_call_state->dispatch_timestamp - processing_time));
    } else {
        HLOG(WARNING) << "processing_time is not set";
    }
    per_func_state->output_size_stat()->AddSample(gsl::narrow_cast<uint32_t>(output_size));
    per_func_call_states_.erase(func_call.full_call_id);
}

void Dispatcher::OnFuncCallFailed(MessageConnection* worker_connection,
                                  const FuncCall& func_call) {
    VLOG(1) << "OnFuncCallFailed " << FuncCallDebugString(func_call);
    uint16_t func_id = func_call.func_id;
    DCHECK_EQ(func_id, worker_connection->func_id());
    absl::MutexLock lk(&mu_);
    DCHECK(per_func_call_states_.contains(func_call.full_call_id));
    DCHECK(per_func_states_.contains(func_id));
    auto per_func_state = per_func_states_[func_id].get();
    FuncWorkerFinished(per_func_state, worker_connection);
    per_func_call_states_.erase(func_call.full_call_id);
}

void Dispatcher::FuncWorkerFinished(PerFuncState* per_func_state,
                                    MessageConnection* worker_connection) {
    per_func_state->WorkerBecomeIdle(worker_connection);
    DispatchPendingFuncCall(worker_connection, per_func_state);
    VLOG(1) << "func_id " << per_func_state->func_id() << ": "
            << "running_workers=" << per_func_state->running_workers() << ", "
            << "idle_workers=" << per_func_state->idle_workers();
}

void Dispatcher::RequestNewFuncWorker(MessageConnection* launcher_connection) {
    uint16_t client_id = next_client_id_.fetch_add(1);
    HLOG(INFO) << "Request new FuncWorker for func_id " << launcher_connection->func_id()
               << " with client_id " << client_id;
    CHECK_LE(client_id, kMaxClientId) << "Reach maximum number of clients!";
    CHECK(ipc::FifoCreate(ipc::GetFuncWorkerInputFifoName(client_id)))
        << "FifoCreate failed";
    CHECK(ipc::FifoCreate(ipc::GetFuncWorkerOutputFifoName(client_id)))
        << "FifoCreate failed";
    Message message = NewCreateFuncWorkerMessage(client_id);
    SetProfilingFieldsInMessage(&message);
    launcher_connection->WriteMessage(message);
}

bool Dispatcher::DispatchPendingFuncCall(MessageConnection* worker_connection,
                                         PerFuncState* per_func_state) {
    Message* pending_invoke_func_message = per_func_state->PeekPendingFuncCall();
    if (pending_invoke_func_message != nullptr) {
        FuncCall func_call = GetFuncCallFromMessage(*pending_invoke_func_message);
        VLOG(1) << "Found pending func_call " << FuncCallDebugString(func_call);
        DispatchFuncCall(worker_connection, pending_invoke_func_message);
        per_func_state->WorkerBecomeRunning(worker_connection, func_call);
        per_func_state->PopPendingFuncCall();
        return true;
    }
    return false;
}

void Dispatcher::DispatchFuncCall(MessageConnection* worker_connection,
                                  Message* invoke_func_message) {
    FuncCall func_call = GetFuncCallFromMessage(*invoke_func_message);
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    DCHECK(per_func_call_states_.contains(func_call.full_call_id));
    auto per_func_call_state = &per_func_call_states_[func_call.full_call_id];
    per_func_call_state->dispatch_timestamp = current_timestamp;
    DCHECK(per_func_states_.contains(func_call.func_id));
    auto per_func_state = per_func_states_[func_call.func_id].get();
    per_func_state->queueing_delay_stat()->AddSample(gsl::narrow_cast<int32_t>(
        current_timestamp - per_func_call_state->recv_timestamp));
    SetProfilingFieldsInMessage(invoke_func_message);
    worker_connection->WriteMessage(*invoke_func_message);
}

Dispatcher::PerFuncState::PerFuncState(uint16_t func_id)
    : func_id_(func_id),
      incoming_requests_stat_(stat::Counter::StandardReportCallback(
          fmt::format("incoming_requests[{}]", func_id))),
      input_size_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          fmt::format("input_size[{}]", func_id))),
      output_size_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          fmt::format("output_size[{}]", func_id))),
      queueing_delay_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("queueing_delay[{}]", func_id))),
      processing_delay_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("processing_delay[{}]", func_id))),
      dispatch_overhead_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("dispatch_overhead[{}]", func_id))) {}

Dispatcher::PerFuncState::~PerFuncState() {}

void Dispatcher::PerFuncState::NewWorker(MessageConnection* worker_connection) {
    uint16_t client_id = worker_connection->client_id();
    DCHECK(!workers_.contains(client_id));
    workers_[client_id] = worker_connection;
    idle_workers_.push_back(worker_connection);
}

void Dispatcher::PerFuncState::RemoveWorker(MessageConnection* worker_connection) {
    uint16_t client_id = worker_connection->client_id();
    DCHECK(workers_.contains(client_id));
    DCHECK(workers_[client_id] == worker_connection);
    workers_.erase(client_id);
}

void Dispatcher::PerFuncState::WorkerBecomeRunning(MessageConnection* worker_connection,
                                                   const FuncCall& func_call) {
    uint16_t client_id = worker_connection->client_id();
    DCHECK(workers_.contains(client_id));
    DCHECK(!running_workers_.contains(client_id));
    running_workers_[client_id] = func_call;
}

void Dispatcher::PerFuncState::WorkerBecomeIdle(MessageConnection* worker_connection) {
    uint16_t client_id = worker_connection->client_id();
    DCHECK(workers_.contains(client_id));
    DCHECK(running_workers_.contains(client_id));
    running_workers_.erase(client_id);
    idle_workers_.push_back(worker_connection);
}

MessageConnection* Dispatcher::PerFuncState::PickIdleWorker() {
    while (!idle_workers_.empty()) {
        MessageConnection* worker_connection = idle_workers_.back();
        idle_workers_.pop_back();
        uint16_t client_id = worker_connection->client_id();
        if (workers_.contains(client_id) && !running_workers_.contains(client_id)) {
            return worker_connection;
        }
    }
    return nullptr;
}

void Dispatcher::PerFuncState::PushPendingFuncCall(const Message& invoke_func_message) {
    pending_func_calls_.push(invoke_func_message);
}

Message* Dispatcher::PerFuncState::PeekPendingFuncCall() {
    if (pending_func_calls_.empty()) {
        return nullptr;
    } else {
        return &pending_func_calls_.front();
    }
}

void Dispatcher::PerFuncState::PopPendingFuncCall() {
    if (!pending_func_calls_.empty()) {
        pending_func_calls_.pop();
    }
}

}  // namespace gateway
}  // namespace faas
