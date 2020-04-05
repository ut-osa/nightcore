#define __FAAS_USED_IN_BINDING
#include "worker/lib/manager.h"

#include "common/time.h"
#include "utils/env_variables.h"

namespace faas {
namespace worker_lib {

using protocol::FuncCall;
using protocol::MessageType;
using protocol::Message;
using protocol::Role;
using protocol::Status;
using protocol::HandshakeMessage;
using protocol::HandshakeResponse;

Manager::Manager()
    : started_(false), func_id_(utils::GetEnvVariableAsInt("FUNC_ID", -1)), client_id_(-1),
      watchdog_input_pipe_fd_(utils::GetEnvVariableAsInt("INPUT_PIPE_FD", -1)),
      watchdog_output_pipe_fd_(utils::GetEnvVariableAsInt("OUTPUT_PIPE_FD", -1)),
      gateway_ipc_path_(utils::GetEnvVariable("GATEWAY_IPC_PATH", "/tmp/faas_gateway")),
      shared_memory_(utils::GetEnvVariable("SHARED_MEMORY_PATH", "/dev/shm/faas")),
      next_handle_value_(0),
      send_gateway_data_callback_set_(false),
      send_watchdog_data_callback_set_(false),
      incoming_func_call_callback_set_(false),
      outcoming_func_call_complete_callback_set_(false),
      processing_delay_stat_(
          stat::StatisticsCollector<uint32_t>::StandardReportCallback("processing_delay")) {
    if (!func_config_.Load(utils::GetEnvVariable("FUNC_CONFIG_FILE"))) {
        LOG(FATAL) << "Failed to load function config file";
    }
    LOG(INFO) << "worker_lib::Manager created";
}

Manager::~Manager() {
    LOG(INFO) << "worker_lib::Manager deleted";
}

void Manager::OnGatewayIOError(int _errno) {
    errno = _errno;
    PLOG(FATAL) << "Gateway IO failed";
}

void Manager::OnGatewayIOError(std::string_view message) {
    LOG(FATAL) << "Gateway IO failed: " << message;
}

void Manager::OnWatchdogIOError(int _errno) {
    errno = _errno;
    PLOG(FATAL) << "Watchdog IO failed";
}

void Manager::OnWatchdogIOError(std::string_view message) {
    LOG(FATAL) << "Watchdog IO failed: " << message;
}

void Manager::Start() {
    DCHECK(send_gateway_data_callback_set_);
    DCHECK(send_watchdog_data_callback_set_);
    DCHECK(incoming_func_call_callback_set_);
    DCHECK(outcoming_func_call_complete_callback_set_);
    started_ = true;
    HandshakeMessage message = {
        .role = static_cast<uint16_t>(Role::FUNC_WORKER),
        .func_id = static_cast<uint16_t>(func_id_)
    };
    send_gateway_data_callback_(std::span<const char>(
        reinterpret_cast<const char*>(&message), sizeof(HandshakeMessage)));
}

void Manager::OnRecvGatewayData(std::span<const char> data) {
    DCHECK(started_);
    if (client_id_ == -1) {
        // Perform handshake with gateway
        gateway_recv_buffer_.AppendData(data);
        if (gateway_recv_buffer_.length() >= sizeof(HandshakeResponse)) {
            HandshakeResponse* response = reinterpret_cast<HandshakeResponse*>(
                gateway_recv_buffer_.data());
            if (static_cast<Status>(response->status) != Status::OK) {
                LOG(FATAL) << "Handshake failed";
            }
            client_id_ = response->client_id;
            LOG(INFO) << "Handshake done";
            gateway_recv_buffer_.ConsumeFront(sizeof(HandshakeResponse));
            DCHECK(gateway_recv_buffer_.length() == 0);
            // Process pending watchdog messages
            while (watchdog_recv_buffer_.length() >= sizeof(Message)) {
                Message* message = reinterpret_cast<Message*>(
                    watchdog_recv_buffer_.data());
                OnRecvWatchdogMessage(*message);
                watchdog_recv_buffer_.ConsumeFront(sizeof(Message));
            }
        }
    } else {
        utils::ReadMessages<Message>(
            &gateway_recv_buffer_, data.data(), data.size(),
            [this] (Message* message) { OnRecvGatewayMessage(*message); });
    }
}

void Manager::OnRecvWatchdogData(std::span<const char> data) {
    DCHECK(started_);
    if (client_id_ == -1) {
        // Handshake with gateway in progress, just save received data in buffer
        watchdog_recv_buffer_.AppendData(data);
    } else {
        utils::ReadMessages<Message>(
            &watchdog_recv_buffer_, data.data(), data.size(),
            [this] (Message* message) { OnRecvWatchdogMessage(*message); });
    }
}

bool Manager::OnOutcomingFuncCall(std::string_view func_name, std::span<const char> input,
                                  uint32_t* handle) {
    DCHECK(started_);
    DCHECK(client_id_ != -1) << "Handshake not done";
    if (input.size() == 0) {
        LOG(ERROR) << "Input is empty";
        return false;
    }
    const FuncConfig::Entry* func_entry = func_config_.find_by_func_name(func_name);
    if (func_entry == nullptr) {
        LOG(ERROR) << "Cannot find function with name " << func_name;
        return false;
    }
    *handle = next_handle_value_++;
    FuncCall func_call;
    func_call.func_id = static_cast<uint16_t>(func_entry->func_id);
    func_call.client_id = client_id_;
    func_call.call_id = *handle;
    auto context = std::make_unique<OutcomingFuncCallContext>();
    context->func_call = func_call;
    context->input_region = shared_memory_.Create(
        utils::SharedMemory::InputPath(func_call.full_call_id), input.size());
    context->output_region = nullptr;
    memcpy(context->input_region->base(), input.data(), input.size());
    outcoming_func_calls_[*handle] = std::move(context);
    Message message = {
#ifdef __FAAS_ENABLE_PROFILING
        .send_timestamp = GetMonotonicMicroTimestamp(),
        .processing_time = 0,
#endif
        .message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC),
        .func_call = func_call
    };
    send_gateway_data_callback_(std::span<const char>(
        reinterpret_cast<const char*>(&message), sizeof(Message)));
    return true;
}

void Manager::OnIncomingFuncCallComplete(uint32_t handle, bool success, std::span<const char> output) {
    DCHECK(started_);
    if (incoming_func_calls_.count(handle) == 0) {
        LOG(FATAL) << "Cannot find incoming function call " << handle;
    }
    IncomingFuncCallContext* context = incoming_func_calls_[handle].get();
    uint32_t processing_time = GetMonotonicMicroTimestamp() - context->start_timestamp;
    processing_delay_stat_.AddSample(processing_time);
    if (success) {
        utils::SharedMemory::Region* output_region = shared_memory_.Create(
            utils::SharedMemory::OutputPath(context->func_call.full_call_id), output.size());
        if (output.size() > 0) {
            memcpy(output_region->base(), output.data(), output.size());
        }
        output_region->Close();
    }
    Message response;
    response.func_call = context->func_call;
    if (success) {
        response.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
    } else {
        response.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
    }
#ifdef __FAAS_ENABLE_PROFILING
    response.send_timestamp = GetMonotonicMicroTimestamp();
    response.processing_time = processing_time;
#endif
    if (success) {
        send_gateway_data_callback_(std::span<const char>(
            reinterpret_cast<const char*>(&response), sizeof(Message)));
    }
    send_watchdog_data_callback_(std::span<const char>(
        reinterpret_cast<const char*>(&response), sizeof(Message)));
    incoming_func_calls_.erase(handle);
}

void Manager::OnRecvGatewayMessage(const protocol::Message& message) {
    MessageType type = static_cast<MessageType>(message.message_type);
    switch (type) {
    case MessageType::FUNC_CALL_COMPLETE:
        OnOutcomingFuncCallComplete(message.func_call, true);
        break;
    case MessageType::FUNC_CALL_FAILED:
        OnOutcomingFuncCallComplete(message.func_call, false);
        break;
    default:
        LOG(ERROR) << "Cannot handle gateway message of type "
                   << static_cast<int>(type);
    }
}

void Manager::OnRecvWatchdogMessage(const protocol::Message& message) {
    MessageType type = static_cast<MessageType>(message.message_type);
    switch (type) {
    case MessageType::INVOKE_FUNC:
        OnIncomingFuncCall(message.func_call);
        break;
    default:
        LOG(ERROR) << "Cannot handle watchdog message of type "
                   << static_cast<int>(type);
    }
}

void Manager::OnOutcomingFuncCallComplete(FuncCall func_call, bool success) {
    uint32_t handle = func_call.call_id;
    if (outcoming_func_calls_.count(handle) == 0) {
        LOG(ERROR) << "Cannot find outcoming function call " << handle;
        return;
    }
    OutcomingFuncCallContext* context = outcoming_func_calls_[handle].get();
    std::span<const char> output;
    if (success) {
        context->output_region = shared_memory_.OpenReadOnly(
            utils::SharedMemory::OutputPath(func_call.full_call_id));
        output = context->output_region->to_span();
    }
    outcoming_func_call_complete_callback_(handle, success, output);
    if (context->input_region != nullptr) {
        context->input_region->Close(true);
    }
    if (context->output_region != nullptr) {
        context->output_region->Close(true);
    }
    outcoming_func_calls_.erase(handle);
}

void Manager::OnIncomingFuncCall(FuncCall func_call) {
    utils::SharedMemory::Region* input_region = shared_memory_.OpenReadOnly(
        utils::SharedMemory::InputPath(func_call.full_call_id));
    auto context = std::make_unique<IncomingFuncCallContext>();
    context->func_call = func_call;
    context->start_timestamp = GetMonotonicMicroTimestamp();
    uint32_t handle = next_handle_value_++;
    incoming_func_calls_[handle] = std::move(context);
    incoming_func_call_callback_(handle, input_region->to_span());
    input_region->Close();
}

}  // namespace worker_lib
}  // namespace faas
