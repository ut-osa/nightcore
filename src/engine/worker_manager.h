#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "engine/message_connection.h"

namespace faas {
namespace engine {

class Engine;
class FuncWorker;

class WorkerManager {
public:
    static constexpr int kDefaultMinWorkersPerFunc = 4;

    explicit WorkerManager(Engine* engine);
    ~WorkerManager();

    // All must be thread-safe
    bool OnLauncherConnected(MessageConnection* launcher_connection);
    void OnLauncherDisconnected(MessageConnection* launcher_connection);
    bool OnFuncWorkerConnected(MessageConnection* worker_connection);
    void OnFuncWorkerDisconnected(MessageConnection* worker_connection);
    bool RequestNewFuncWorker(uint16_t func_id, uint16_t* client_id);
    std::shared_ptr<FuncWorker> GetFuncWorker(uint16_t client_id);

private:
    Engine* engine_;
    std::atomic<uint16_t> next_client_id_;

    absl::Mutex mu_;
    absl::flat_hash_map</* func_id */ uint16_t, std::shared_ptr<server::ConnectionBase>>
        launcher_connections_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* client_id */ uint16_t, std::shared_ptr<FuncWorker>>
        func_workers_ ABSL_GUARDED_BY(mu_);
    
    bool RequestNewFuncWorkerInternal(MessageConnection* launcher_connection, uint16_t* client_id);

    DISALLOW_COPY_AND_ASSIGN(WorkerManager);
};

class FuncWorker {
public:
    explicit FuncWorker(MessageConnection* message_connection);
    ~FuncWorker();

    uint16_t func_id() const { return func_id_; }
    uint16_t client_id() const { return client_id_; }

    // Must be thread-safe
    void SendMessage(protocol::Message* message);

private:
    uint16_t func_id_;
    uint16_t client_id_;
    std::shared_ptr<server::ConnectionBase> message_connection_;

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace engine
}  // namespace faas
