#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "gateway/message_connection.h"

namespace faas {
namespace gateway {

class Dispatcher {
public:
    static constexpr int kDefaultMinWorkersPerFunc = 2;
    static constexpr int kMaxClientId = (1 << protocol::kClientIdBits) - 1;

    Dispatcher();
    ~Dispatcher();

    void set_min_workers_per_func(int value) {
        min_workers_per_func_ = value;
    }

    // All must be thread-safe
    bool OnLauncherConnected(MessageConnection* launcher_connection);
    bool OnFuncWorkerConnected(MessageConnection* worker_connection);
    void OnLauncherDisconnected(MessageConnection* launcher_connection);
    void OnFuncWorkerDisconnected(MessageConnection* worker_connection);
    bool OnNewFuncCall(MessageConnection* caller_connection,
                       const protocol::FuncCall& func_call);
    void OnFuncCallCompleted(MessageConnection* worker_connection,
                             const protocol::FuncCall& func_call);
    void OnFuncCallFailed(MessageConnection* worker_connection,
                          const protocol::FuncCall& func_call);

private:
    int min_workers_per_func_;
    std::atomic<uint16_t> next_client_id_;

    absl::Mutex mu_;
    absl::flat_hash_map</* func_id */ uint16_t, MessageConnection*>
        launcher_connections_ ABSL_GUARDED_BY(mu_);

    class PerFuncState {
    public:
        explicit PerFuncState(uint16_t func_id);
        ~PerFuncState();

        size_t total_workers() const { return workers_.size(); }
        size_t running_workers() const { return running_workers_.size(); }
        size_t idle_workers() const { return total_workers() - running_workers(); }

        void NewWorker(MessageConnection* worker_connection);
        void RemoveWorker(MessageConnection* worker_connection);
        void WorkerBecomeRunning(MessageConnection* worker_connection,
                                 const protocol::FuncCall& func_call);
        void WorkerBecomeIdle(MessageConnection* worker_connection);
        MessageConnection* PickIdleWorker();

        void PushPendingFuncCall(const protocol::FuncCall& func_call);
        bool PopPendingFuncCall(protocol::FuncCall* func_call);

    private:
        uint16_t func_id_;
        absl::flat_hash_map</* client_id */ uint16_t, MessageConnection*> workers_;
        absl::flat_hash_map</* client_id */ uint16_t, protocol::FuncCall> running_workers_;
        std::vector<MessageConnection*> idle_workers_;
        std::queue<protocol::FuncCall> pending_func_calls_;

        DISALLOW_COPY_AND_ASSIGN(PerFuncState);
    };

    absl::flat_hash_map</* client_id */ uint16_t, std::unique_ptr<PerFuncState>>
        per_func_states_ ABSL_GUARDED_BY(mu_);

    void FuncWorkerFinished(PerFuncState* per_func_state, MessageConnection* worker_connection);
    void RequestNewFuncWorker(MessageConnection* launcher_connection);
    void DispatchFuncCall(MessageConnection* worker_connection,
                          const protocol::FuncCall& func_call);

    DISALLOW_COPY_AND_ASSIGN(Dispatcher);
};

}  // namespace gateway
}  // namespace faas
