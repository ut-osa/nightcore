#pragma once

#include "base/common.h"
#include "common/func_config.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "utils/shared_memory.h"

namespace faas {
namespace worker_lib {

class Manager {
public:
    Manager();
    ~Manager();

    static constexpr uint32_t kInvalidHandle = std::numeric_limits<uint32_t>::max();

    // All callbacks have to be set before calling Start()
    void Start();

    void OnGatewayIOError(int errnum);
    void OnGatewayIOError(std::string_view message);
    void OnWatchdogIOError(int errnum);
    void OnWatchdogIOError(std::string_view message);

    bool is_async_mode() const { return is_async_mode_; }
    bool is_grpc_service() const { return my_func_config_->is_grpc_service; }
    std::string_view grpc_service_name() const { return my_func_config_->grpc_service_name; }
    int watchdog_input_pipe_fd() const { return watchdog_input_pipe_fd_; }
    int watchdog_output_pipe_fd() const { return watchdog_output_pipe_fd_; }
    std::string_view gateway_ipc_path() const { return gateway_ipc_path_; }

    typedef std::function<void(std::span<const char> /* data */)> SendDataCallback;
    typedef std::function<void(uint32_t /* handle */, std::span<const char> /* input */)>
            IncomingFuncCallCallback;
    typedef std::function<void(uint32_t /* handle */, std::string_view /* method */,
                               std::span<const char> /* request */)>
            IncomingGrpcCallCallback;
    // If reclaim_output_later is set to true by the callback function,
    // ReclaimOutcomingFuncCallOutput should be called later to free output buffer.
    // Otherwise, the output buffer must not be touched after the callback returns.
    // reclaim_output_later is false by default.
    typedef std::function<void(uint32_t /* handle */, bool /* success */,
                               std::span<const char> /* output */,
                               bool* /* reclaim_output_later */)>
            OutcomingFuncCallCompleteCallback;

    void SetSendGatewayDataCallback(SendDataCallback callback) {
        send_gateway_data_callback_set_ = true;
        send_gateway_data_callback_ = callback;
    }
    void SetSendWatchdogDataCallback(SendDataCallback callback) {
        send_watchdog_data_callback_set_ = true;
        send_watchdog_data_callback_ = callback;
    }
    void SetIncomingFuncCallCallback(IncomingFuncCallCallback callback) {
        incoming_func_call_callback_set_ = true;
        incoming_func_call_callback_ = callback;
    }
    void SetIncomingGrpcCallCallback(IncomingGrpcCallCallback callback) {
        incoming_grpc_call_callback_set_ = true;
        incoming_grpc_call_callback_ = callback;
    }
    void SetOutcomingFuncCallCompleteCallback(OutcomingFuncCallCompleteCallback callback) {
        outcoming_func_call_complete_callback_set_ = true;
        outcoming_func_call_complete_callback_ = callback;
    }

    void OnRecvGatewayData(std::span<const char> data);
    void OnRecvWatchdogData(std::span<const char> data);
    bool OnOutcomingFuncCall(std::string_view func_name, std::span<const char> input, uint32_t* handle);
    bool OnOutcomingGrpcCall(std::string_view service, std::string_view method,
                             std::span<const char> request, uint32_t* handle);
    void OnIncomingFuncCallComplete(uint32_t handle, bool success, std::span<const char> output);

    void ReclaimOutcomingFuncCallOutput(uint32_t handle);

private:
    bool started_;
    bool is_async_mode_;
    FuncConfig func_config_;
    const FuncConfig::Entry* my_func_config_;
    int client_id_;
    int watchdog_input_pipe_fd_;
    int watchdog_output_pipe_fd_;
    std::string gateway_ipc_path_;
    utils::SharedMemory shared_memory_;
    uint32_t next_handle_value_;

    bool send_gateway_data_callback_set_;
    SendDataCallback send_gateway_data_callback_;
    bool send_watchdog_data_callback_set_;
    SendDataCallback send_watchdog_data_callback_;
    bool incoming_func_call_callback_set_;
    IncomingFuncCallCallback incoming_func_call_callback_;
    bool incoming_grpc_call_callback_set_;
    IncomingGrpcCallCallback incoming_grpc_call_callback_;
    bool outcoming_func_call_complete_callback_set_;
    OutcomingFuncCallCompleteCallback outcoming_func_call_complete_callback_;

    utils::AppendableBuffer gateway_recv_buffer_;
    utils::AppendableBuffer watchdog_recv_buffer_;

    struct OutcomingFuncCallContext {
        protocol::FuncCall func_call;
        utils::SharedMemory::Region* input_region;
        utils::SharedMemory::Region* output_region;
#ifdef __FAAS_ENABLE_PROFILING
        int64_t start_timestamp;
#endif
    };
    std::unordered_map<uint32_t, std::unique_ptr<OutcomingFuncCallContext>>
        outcoming_func_calls_;

    struct IncomingFuncCallContext {
        protocol::FuncCall func_call;
        utils::SharedMemory::Region* input_region;
        int64_t start_timestamp;
    };
    std::unordered_map<uint32_t, std::unique_ptr<IncomingFuncCallContext>>
        incoming_func_calls_;
    std::unordered_map<uint32_t, utils::SharedMemory::Region*>
        output_regions_to_close_;

    stat::StatisticsCollector<int32_t> gateway_message_delay_stat_;
    stat::StatisticsCollector<int32_t> watchdog_message_delay_stat_;
    stat::StatisticsCollector<int32_t> processing_delay_stat_;
    stat::StatisticsCollector<int32_t> system_protocol_overhead_stat_;
    stat::StatisticsCollector<uint32_t> input_size_stat_;
    stat::StatisticsCollector<uint32_t> output_size_stat_;
    stat::Counter incoming_requests_counter_;

    void OnRecvGatewayMessage(const protocol::Message& message);
    void OnRecvWatchdogMessage(const protocol::Message& message);
    void OnOutcomingFuncCallComplete(protocol::FuncCall func_call, bool success,
                                     int32_t processing_time = 0);
    void OnIncomingFuncCall(protocol::FuncCall func_call);

    DISALLOW_COPY_AND_ASSIGN(Manager);
};

}  // namespace worker_lib
}  // namespace faas
