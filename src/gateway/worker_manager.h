#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "gateway/message_connection.h"

namespace faas {
namespace gateway {

class Server;
class FuncWorker;

class WorkerManager {
public:
    static constexpr int kDefaultMinWorkersPerFunc = 4;
    static constexpr int kMaxClientId = (1 << protocol::kClientIdBits) - 1;

    explicit WorkerManager(Server* server);
    ~WorkerManager();

    // All must be thread-safe
    bool OnLauncherConnected(MessageConnection* launcher_connection,
                             std::string_view container_id);
    void OnLauncherDisconnected(MessageConnection* launcher_connection);
    bool OnFuncWorkerConnected(MessageConnection* worker_connection);
    void OnFuncWorkerDisconnected(MessageConnection* worker_connection);
    bool RequestNewFuncWorker(uint16_t func_id);

private:
    Server* server_;
    std::atomic<uint16_t> next_client_id_;

    absl::Mutex mu_;
    absl::flat_hash_map</* func_id */ uint16_t, MessageConnection*>
        launcher_connections_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* func_id */ uint16_t, std::string>
        func_container_ids_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* client_id */ uint16_t, std::unique_ptr<FuncWorker>>
        func_workers_ ABSL_GUARDED_BY(mu_);
    
    bool RequestNewFuncWorkerInternal(MessageConnection* launcher_connection);

    DISALLOW_COPY_AND_ASSIGN(WorkerManager);
};

class FuncWorker {
public:
    explicit FuncWorker(MessageConnection* message_connection);
    ~FuncWorker();

    uint16_t func_id() const { return func_id_; }
    uint16_t client_id() const { return client_id_; }

    // Must be thread-safe
    void DispatchFuncCall(protocol::Message* invoke_func_message);

private:
    uint16_t func_id_;
    uint16_t client_id_;
    MessageConnection* connection_;

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace gateway
}  // namespace faas
