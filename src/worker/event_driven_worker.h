#pragma once

#include "base/common.h"
#include "common/func_config.h"
#include "common/protocol.h"
#include "ipc/shm_region.h"
#include "utils/object_pool.h"

namespace faas {
namespace worker_lib {

class EventDrivenWorker {
public:
    EventDrivenWorker();
    ~EventDrivenWorker();

    void Start();

    bool is_grpc_service() { return config_entry_->is_grpc_service; }
    std::string_view func_name() { return config_entry_->func_name; }
    std::string_view grpc_service_name() { return config_entry_->grpc_service_name; }

    typedef std::function<void(int /* fd */)> WatchFdReadableCallback;
    void SetWatchFdReadableCallback(WatchFdReadableCallback callback) {
        watch_fd_readable_cb_ = callback;
    }

    typedef std::function<void(int /* fd */)> StopWatchFdCallback;
    void SetStopWatchFdCallback(StopWatchFdCallback callback) {
        stop_watch_fd_cb_ = callback;
    }

    typedef std::function<void(int64_t /* handle */, std::string_view /* method */,
                               std::span<const char> /* request */)>
            IncomingFuncCallCallback;
    void SetIncomingFuncCallCallback(IncomingFuncCallCallback callback) {
        incoming_func_call_cb_ = callback;
    }

    typedef std::function<void(int64_t /* handle */, bool /* success */,
                               std::span<const char> /* output */)>
            OutgoingFuncCallCompleteCallback;
    void SetOutgoingFuncCallCompleteCallback(OutgoingFuncCallCompleteCallback callback) {
        outgoing_func_call_complete_cb_ = callback;
    }

    void OnFdReadable(int fd);

    void OnFuncExecutionFinished(int64_t handle, bool success, std::span<const char> output);
    bool NewOutgoingFuncCall(int64_t parent_handle, std::string_view func_name,
                             std::span<const char> input, int64_t* handle);
    bool NewOutgoingGrpcCall(int64_t parent_handle, std::string_view service,
                             std::string_view method, std::span<const char> request,
                             int64_t* handle);

private:
    WatchFdReadableCallback           watch_fd_readable_cb_;
    StopWatchFdCallback               stop_watch_fd_cb_;
    IncomingFuncCallCallback          incoming_func_call_cb_;
    OutgoingFuncCallCompleteCallback  outgoing_func_call_complete_cb_;

    bool use_fifo_for_nested_call_;
    int message_pipe_fd_;
    FuncConfig func_config_;
    const FuncConfig::Entry* config_entry_;
    uint16_t initial_client_id_;
    char main_pipe_buf_[PIPE_BUF];

    struct FuncWorkerState {
        uint16_t client_id;
        int      engine_sock_fd;
        int      input_pipe_fd;
        int      output_pipe_fd;
        uint32_t next_call_id;
    };
    std::unordered_map</* client_id */ uint16_t, std::unique_ptr<FuncWorkerState>>
        func_workers_;
    std::unordered_map</* input_pipe_fd */ int, FuncWorkerState*>
        func_worker_by_input_fd_;

    struct IncomingFuncCallState {
        protocol::FuncCall func_call;
        uint16_t           recv_client_id;
        int32_t            dispatch_delay;
        int64_t            start_timestamp;
    };
    utils::SimpleObjectPool<IncomingFuncCallState> incoming_func_call_pool_;
    std::unordered_map</* full_call_id */ uint64_t, IncomingFuncCallState*>
        incoming_func_calls_;

    struct OutgoingFuncCallState {
        protocol::FuncCall              func_call;
        std::unique_ptr<ipc::ShmRegion> input_region;
        int                             output_pipe_fd;
    };
    utils::SimpleObjectPool<OutgoingFuncCallState> outgoing_func_call_pool_;
    std::unordered_map</* full_call_id */ uint64_t, OutgoingFuncCallState*>
        outgoing_func_calls_;
    std::unordered_map</* output_pipe_fd */ int, OutgoingFuncCallState*>
        outgoing_func_call_by_output_pipe_fd_;

    inline int64_t func_call_to_handle(const protocol::FuncCall& func_call) {
        return gsl::narrow_cast<int64_t>(func_call.full_call_id);
    }
    inline protocol::FuncCall handle_to_func_call(int64_t handle) {
        protocol::FuncCall func_call;
        func_call.full_call_id = gsl::narrow_cast<uint64_t>(handle);
        return func_call;
    }

    FuncWorkerState* GetAssociatedFuncWorkerState(const protocol::FuncCall& incoming_func_call);
    void NewFuncWorker(uint16_t client_id);
    void ExecuteFunc(FuncWorkerState* state, const protocol::Message& dispatch_func_call_message);
    bool NewOutgoingFuncCallCommon(const protocol::FuncCall& parent_call,
                                   const protocol::FuncCall& func_call,
                                   FuncWorkerState* worker_state, std::span<const char> input);

    void OnMessagePipeReadable();
    void OnEnginePipeReadable(FuncWorkerState* state);
    void OnOutputPipeReadable(OutgoingFuncCallState* state);
    void OnOutgoingFuncCallFinished(const protocol::Message& message, OutgoingFuncCallState* state);

    DISALLOW_COPY_AND_ASSIGN(EventDrivenWorker);
};

}  // namespace worker_lib
}  // namespace faas
