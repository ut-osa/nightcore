#pragma once

#include "base/common.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "gateway/message_connection.h"

namespace faas {
namespace gateway {

class Server;

class Dispatcher {
public:
    static constexpr int kDefaultMinWorkersPerFunc = 4;
    static constexpr int kMaxClientId = (1 << protocol::kClientIdBits) - 1;

    explicit Dispatcher(Server* server);
    ~Dispatcher();

    // All must be thread-safe
    bool OnLauncherConnected(MessageConnection* launcher_connection);
    bool OnFuncWorkerConnected(MessageConnection* worker_connection);
    void OnLauncherDisconnected(MessageConnection* launcher_connection);
    void OnFuncWorkerDisconnected(MessageConnection* worker_connection);
    bool OnNewFuncCall(MessageConnection* caller_connection,
                       const protocol::FuncCall& func_call, size_t input_size,
                       std::span<const char> inline_input, bool shm_input);
    void OnFuncCallCompleted(MessageConnection* worker_connection,
                             const protocol::FuncCall& func_call,
                             int32_t processing_time, size_t output_size);
    void OnFuncCallFailed(MessageConnection* worker_connection,
                          const protocol::FuncCall& func_call);

private:
    Server* server_;
    std::atomic<uint16_t> next_client_id_;

    absl::Mutex mu_;
    absl::flat_hash_map</* func_id */ uint16_t, MessageConnection*>
        launcher_connections_ ABSL_GUARDED_BY(mu_);

    class PerFuncState {
    public:
        explicit PerFuncState(uint16_t func_id);
        ~PerFuncState();

        int16_t func_id() const { return func_id_; }
        size_t total_workers() const { return workers_.size(); }
        size_t running_workers() const { return running_workers_.size(); }
        size_t idle_workers() const { return total_workers() - running_workers(); }

        void NewWorker(MessageConnection* worker_connection);
        void RemoveWorker(MessageConnection* worker_connection);
        void WorkerBecomeRunning(MessageConnection* worker_connection,
                                 const protocol::FuncCall& func_call);
        void WorkerBecomeIdle(MessageConnection* worker_connection);
        MessageConnection* PickIdleWorker();

        void PushPendingFuncCall(const protocol::Message& invoke_func_message);
        protocol::Message* PeekPendingFuncCall();
        void PopPendingFuncCall();

        stat::Counter* incoming_requests_stat() {
            return &incoming_requests_stat_;
        }

        stat::StatisticsCollector<uint32_t>* input_size_stat() {
            return &input_size_stat_;
        }

        stat::StatisticsCollector<uint32_t>* output_size_stat() {
            return &output_size_stat_;
        }

        stat::StatisticsCollector<int32_t>* queueing_delay_stat() {
            return &queueing_delay_stat_;
        }

        stat::StatisticsCollector<int32_t>* processing_delay_stat() {
            return &processing_delay_stat_;
        }

        stat::StatisticsCollector<int32_t>* dispatch_overhead_stat() {
            return &dispatch_overhead_stat_;
        }

    private:
        uint16_t func_id_;
        absl::flat_hash_map</* client_id */ uint16_t, MessageConnection*> workers_;
        absl::flat_hash_map</* client_id */ uint16_t, protocol::FuncCall> running_workers_;
        std::vector<MessageConnection*> idle_workers_;
        std::queue<protocol::Message> pending_func_calls_;

        stat::Counter incoming_requests_stat_;
        stat::StatisticsCollector<uint32_t> input_size_stat_;
        stat::StatisticsCollector<uint32_t> output_size_stat_;
        stat::StatisticsCollector<int32_t> queueing_delay_stat_;
        stat::StatisticsCollector<int32_t> processing_delay_stat_;
        stat::StatisticsCollector<int32_t> dispatch_overhead_stat_;

        DISALLOW_COPY_AND_ASSIGN(PerFuncState);
    };

    absl::flat_hash_map</* client_id */ uint16_t, std::unique_ptr<PerFuncState>>
        per_func_states_ ABSL_GUARDED_BY(mu_);

    struct PerFuncCallState {
        int64_t recv_timestamp;
        int64_t dispatch_timestamp;
    };
    absl::flat_hash_map</* full_call_id */ uint64_t, PerFuncCallState>
        per_func_call_states_ ABSL_GUARDED_BY(mu_);

    void FuncWorkerFinished(PerFuncState* per_func_state,
                            MessageConnection* worker_connection) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void RequestNewFuncWorker(MessageConnection* launcher_connection) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    bool DispatchPendingFuncCall(MessageConnection* worker_connection,
                                 PerFuncState* per_func_state) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void DispatchFuncCall(MessageConnection* worker_connection,
                          protocol::Message* invoke_func_message) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

    DISALLOW_COPY_AND_ASSIGN(Dispatcher);
};

}  // namespace gateway
}  // namespace faas
