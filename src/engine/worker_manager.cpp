#include "engine/worker_manager.h"

#include "ipc/base.h"
#include "ipc/fifo.h"
#include "engine/engine.h"

#define HLOG(l) LOG(l) << "WorkerManager: "
#define HVLOG(l) VLOG(l) << "WorkerManager: "

namespace faas {
namespace engine {

using protocol::Message;
using protocol::NewCreateFuncWorkerMessage;

WorkerManager::WorkerManager(Engine* engine)
    : engine_(engine), next_client_id_(1) {}

WorkerManager::~WorkerManager() {}

bool WorkerManager::OnLauncherConnected(MessageConnection* launcher_connection) {
    uint16_t func_id = launcher_connection->func_id();
    HLOG(INFO) << fmt::format("Launcher of func_id {} connected", func_id);
    {
        absl::MutexLock lk(&mu_);
        if (launcher_connections_.contains(func_id)) {
            HLOG(ERROR) << fmt::format("Launcher of func_id {} already connected", func_id);
            return false;
        }
        launcher_connections_[func_id] = launcher_connection->ref_self();
    }
    const FuncConfig::Entry* func_entry = engine_->func_config()->find_by_func_id(func_id);
    DCHECK(func_entry != nullptr);
    int min_workers = func_entry->min_workers;
    if (min_workers == -1) {
        min_workers = kDefaultMinWorkersPerFunc;
    }
    for (int i = 0; i < min_workers; i++) {
        uint16_t client_id;
        RequestNewFuncWorkerInternal(launcher_connection, &client_id);
    }
    return true;
}

void WorkerManager::OnLauncherDisconnected(MessageConnection* launcher_connection) {
    uint16_t func_id = launcher_connection->func_id();
    HLOG(INFO) << fmt::format("Launcher of func_id {} disconnected", func_id);
    absl::MutexLock lk(&mu_);
    if (launcher_connections_.contains(func_id)
          && launcher_connections_[func_id]->as_ptr<MessageConnection>() == launcher_connection) {
        launcher_connections_.erase(func_id);
    } else {
        HLOG(ERROR) << fmt::format("Cannot find launcher connection for func_id {}", func_id);
    }
}

bool WorkerManager::OnFuncWorkerConnected(MessageConnection* worker_connection) {
    uint16_t func_id = worker_connection->func_id();
    uint16_t client_id = worker_connection->client_id();
    HLOG(INFO) << fmt::format("FuncWorker of func_id {}, client_id {} connected",
                              func_id, client_id);
    std::shared_ptr<FuncWorker> func_worker;
    {
        absl::MutexLock lk(&mu_);
        if (func_workers_.contains(client_id)) {
            HLOG(ERROR) << fmt::format("FuncWorker of client_id {} already exists", client_id);
            return false;
        }
        func_worker = std::make_shared<FuncWorker>(worker_connection);
        func_workers_[client_id] = func_worker;
    }
    Dispatcher* dispatcher = engine_->GetOrCreateDispatcher(func_id);
    if (dispatcher == nullptr || !dispatcher->OnFuncWorkerConnected(func_worker)) {
        absl::MutexLock lk(&mu_);
        func_workers_.erase(client_id);
        return false;
    }
    return true;
}

void WorkerManager::OnFuncWorkerDisconnected(MessageConnection* worker_connection) {
    uint16_t func_id = worker_connection->func_id();
    uint16_t client_id = worker_connection->client_id();
    HLOG(INFO) << fmt::format("FuncWorker of func_id {}, client_id {} disconnected",
                              func_id, client_id);
    std::shared_ptr<FuncWorker> func_worker;
    {
        absl::MutexLock lk(&mu_);
        if (!func_workers_.contains(client_id)) {
            HLOG(WARNING) << fmt::format("FuncWorker of client_id {} does not exist", client_id);
            return;
        }
        func_worker = std::move(func_workers_[client_id]);
        func_workers_.erase(client_id);
    }
    Dispatcher* dispatcher = engine_->GetOrCreateDispatcher(func_id);
    if (dispatcher != nullptr) {
        dispatcher->OnFuncWorkerDisconnected(func_worker.get());
    }
    ipc::FifoRemove(ipc::GetFuncWorkerInputFifoName(client_id));
    ipc::FifoRemove(ipc::GetFuncWorkerOutputFifoName(client_id));
}

bool WorkerManager::RequestNewFuncWorker(uint16_t func_id, uint16_t* client_id) {
    std::shared_ptr<server::ConnectionBase> connection;
    {
        absl::MutexLock lk(&mu_);
        if (!launcher_connections_.contains(func_id)) {
            HLOG(ERROR) << fmt::format("Cannot find launcher connection for func_id {}", func_id);
            return false;
        }
        connection = launcher_connections_[func_id];
    }
    return RequestNewFuncWorkerInternal(connection->as_ptr<MessageConnection>(), client_id);
}

std::shared_ptr<FuncWorker> WorkerManager::GetFuncWorker(uint16_t client_id) {
    absl::MutexLock lk(&mu_);
    if (!func_workers_.contains(client_id)) {
        HLOG(WARNING) << fmt::format("FuncWorker of client_id {} does not exist", client_id);
        return nullptr;
    } else {
        return func_workers_[client_id];
    }
}

bool WorkerManager::RequestNewFuncWorkerInternal(MessageConnection* launcher_connection,
                                                 uint16_t* out_client_id) {
    uint16_t client_id = next_client_id_.fetch_add(1);
    CHECK_LE(client_id, protocol::kMaxClientId) << "Reach maximum number of clients!";
    HLOG(INFO) << fmt::format("Request new FuncWorker for func_id {} with client_id {}",
                              launcher_connection->func_id(), client_id);
    if (!engine_->func_worker_use_engine_socket()) {
        CHECK(ipc::FifoCreate(ipc::GetFuncWorkerInputFifoName(client_id)))
            << "FifoCreate failed";
        CHECK(ipc::FifoCreate(ipc::GetFuncWorkerOutputFifoName(client_id)))
            << "FifoCreate failed";
    }
    Message message = NewCreateFuncWorkerMessage(client_id);
    launcher_connection->WriteMessage(message);
    *out_client_id = client_id;
    return true;
}

FuncWorker::FuncWorker(MessageConnection* message_connection)
    : func_id_(message_connection->func_id()),
      client_id_(message_connection->client_id()),
      message_connection_(message_connection->ref_self()) {}

FuncWorker::~FuncWorker() {}

void FuncWorker::SendMessage(Message* message) {
    message->send_timestamp = GetMonotonicMicroTimestamp();
    message_connection_->as_ptr<MessageConnection>()->WriteMessage(*message);
}

}  // namespace engine
}  // namespace faas
