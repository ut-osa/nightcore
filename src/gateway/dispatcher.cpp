#include "gateway/dispatcher.h"

#define HLOG(l) LOG(l) << "Dispatcher: "
#define HVLOG(l) VLOG(l) << "Dispatcher: "

namespace faas {
namespace gateway {

using protocol::FuncCall;
using protocol::Message;
using protocol::NewCreateFuncWorkerMessage;
using protocol::NewInvokeFuncMessage;

constexpr int Dispatcher::kDefaultMinWorkersPerFunc;
constexpr int Dispatcher::kMaxClientId;

Dispatcher::Dispatcher()
    : min_workers_per_func_(kDefaultMinWorkersPerFunc),
      next_client_id_(1) {}

Dispatcher::~Dispatcher() {}

bool Dispatcher::OnLauncherConnected(MessageConnection* launcher_connection) {
    uint16_t func_id = launcher_connection->func_id();
    absl::MutexLock lk(&mu_);
    if (launcher_connections_.contains(func_id)) {
        HLOG(ERROR) << "Launcher of func_id " << func_id << " already connected";
        return false;
    }
    launcher_connections_[func_id] = launcher_connection;
    per_func_states_[func_id] = std::make_unique<PerFuncState>(func_id);
    for (int i = 0; i < min_workers_per_func_; i++) {
        RequestNewFuncWorker(launcher_connection);
    }
    return true;
}

bool Dispatcher::OnFuncWorkerConnected(MessageConnection* worker_connection) {
    uint16_t func_id = worker_connection->func_id();
    absl::MutexLock lk(&mu_);
    if (!per_func_states_.contains(func_id)) {
        HLOG(ERROR) << "PerFuncState of func_id " << func_id << " does not exist";
        return false;
    }
    per_func_states_[func_id]->NewWorker(worker_connection);
    return true;
}

void Dispatcher::OnLauncherDisconnected(MessageConnection* launcher_connection) {
    uint16_t func_id = launcher_connection->func_id();
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
    absl::MutexLock lk(&mu_);
    DCHECK(per_func_states_.contains(func_id));
    per_func_states_[func_id]->RemoveWorker(worker_connection);
}

bool Dispatcher::OnNewFuncCall(MessageConnection* caller_connection,
                               const FuncCall& func_call) {
    uint16_t func_id = func_call.func_id;
    absl::MutexLock lk(&mu_);
    if (!launcher_connections_.contains(func_id)) {
        return false;
    }
    DCHECK(per_func_states_.contains(func_id));
    auto per_func_state = per_func_states_[func_id].get();
    MessageConnection* idle_worker = per_func_state->PickIdleWorker();
    if (idle_worker) {
        DispatchFuncCall(idle_worker, func_call);
        per_func_state->WorkerBecomeRunning(idle_worker, func_call);
    } else {
        per_func_state->PushPendingFuncCall(func_call);
    }
    return true;
}

void Dispatcher::OnFuncCallCompleted(MessageConnection* worker_connection,
                                     const FuncCall& func_call) {
    uint16_t func_id = func_call.func_id;
    DCHECK_EQ(func_id, worker_connection->func_id());
    absl::MutexLock lk(&mu_);
    DCHECK(per_func_states_.contains(func_id));
    auto per_func_state = per_func_states_[func_id].get();
    FuncWorkerFinished(per_func_state, worker_connection);
}

void Dispatcher::OnFuncCallFailed(MessageConnection* worker_connection,
                                  const FuncCall& func_call) {
    uint16_t func_id = func_call.func_id;
    DCHECK_EQ(func_id, worker_connection->func_id());
    absl::MutexLock lk(&mu_);
    DCHECK(per_func_states_.contains(func_id));
    auto per_func_state = per_func_states_[func_id].get();
    FuncWorkerFinished(per_func_state, worker_connection);
}

void Dispatcher::FuncWorkerFinished(PerFuncState* per_func_state,
                                    MessageConnection* worker_connection) {
    per_func_state->WorkerBecomeIdle(worker_connection);
    FuncCall pending_func_call;
    if (per_func_state->PopPendingFuncCall(&pending_func_call)) {
        per_func_state->WorkerBecomeRunning(worker_connection, pending_func_call);
    }
}

void Dispatcher::RequestNewFuncWorker(MessageConnection* launcher_connection) {
    uint16_t client_id = next_client_id_.fetch_add(1);
    CHECK_LE(client_id, kMaxClientId) << "Reach maximum number of clients!";
    Message message = NewCreateFuncWorkerMessage(client_id);
#ifdef __FAAS_ENABLE_PROFILING
    message->send_timestamp = GetMonotonicMicroTimestamp();
    message->processing_time = 0;
#endif
    launcher_connection->WriteMessage(message);
}

void Dispatcher::DispatchFuncCall(MessageConnection* worker_connection,
                                  const FuncCall& func_call) {
    Message message = NewInvokeFuncMessage(func_call);
#ifdef __FAAS_ENABLE_PROFILING
    message->send_timestamp = GetMonotonicMicroTimestamp();
    message->processing_time = 0;
#endif
    worker_connection->WriteMessage(message);
}

Dispatcher::PerFuncState::PerFuncState(uint16_t func_id)
    : func_id_(func_id) {}

Dispatcher::PerFuncState::~PerFuncState() {}

void Dispatcher::PerFuncState::NewWorker(MessageConnection* worker_connection) {
    uint16_t client_id = worker_connection->client_id();
    DCHECK(!workers_.contains(client_id));
    workers_[client_id] = worker_connection;
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

void Dispatcher::PerFuncState::PushPendingFuncCall(const FuncCall& func_call) {
    pending_func_calls_.push(func_call);
}

bool Dispatcher::PerFuncState::PopPendingFuncCall(FuncCall* func_call) {
    if (pending_func_calls_.empty()) {
        return false;
    } else {
        *func_call = pending_func_calls_.front();
        pending_func_calls_.pop();
        return true;
    }
}

}  // namespace gateway
}  // namespace faas
