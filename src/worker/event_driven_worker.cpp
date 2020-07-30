#define __FAAS_USED_IN_BINDING
#include "worker/event_driven_worker.h"

#include "common/time.h"
#include "ipc/base.h"
#include "ipc/fifo.h"
#include "utils/io.h"
#include "utils/socket.h"
#include "utils/env_variables.h"
#include "worker/worker_lib.h"

namespace faas {
namespace worker_lib {

using protocol::FuncCall;
using protocol::NewFuncCall;
using protocol::NewFuncCallWithMethod;
using protocol::FuncCallDebugString;
using protocol::Message;
using protocol::GetFuncCallFromMessage;
using protocol::GetInlineDataFromMessage;
using protocol::IsHandshakeResponseMessage;
using protocol::IsCreateFuncWorkerMessage;
using protocol::IsDispatchFuncCallMessage;
using protocol::IsFuncCallCompleteMessage;
using protocol::IsFuncCallFailedMessage;
using protocol::NewFuncWorkerHandshakeMessage;
using protocol::NewFuncCallFailedMessage;

static std::atomic<int> worker_created{0};

EventDrivenWorker::EventDrivenWorker() {
    int zero = 0;
    if (!worker_created.compare_exchange_strong(zero, 1)) {
        LOG(FATAL) << "More than one EventDrivenWorker created";
    }

    use_fifo_for_nested_call_ = false;

    ipc::SetRootPathForIpc(utils::GetEnvVariable("FAAS_ROOT_PATH_FOR_IPC", ""));
    int func_id = utils::GetEnvVariableAsInt("FAAS_FUNC_ID", -1);
    CHECK(func_id != -1) << "FAAS_FUNC_ID is not set";
    int client_id = utils::GetEnvVariableAsInt("FAAS_CLIENT_ID", -1);
    CHECK(client_id != -1) << "FAAS_CLIENT_ID is not set";
    message_pipe_fd_ = utils::GetEnvVariableAsInt("FAAS_MSG_PIPE_FD", -1);
    CHECK(message_pipe_fd_ != -1) << "FAAS_MSG_PIPE_FD is not set";

    // Initialize function configs
    uint32_t payload_size;
    CHECK(io_utils::RecvData(message_pipe_fd_, reinterpret_cast<char*>(&payload_size),
                             sizeof(uint32_t), /* eof= */ nullptr))
        << "Failed to receive payload size from launcher";
    char* payload = reinterpret_cast<char*>(malloc(payload_size));
    auto reclaim_payload_buffer = gsl::finally([payload] { free(payload); });
    CHECK(io_utils::RecvData(message_pipe_fd_, payload, payload_size, /* eof= */ nullptr))
        << "Failed to receive payload data from launcher";
    CHECK(func_config_.Load(std::string_view(payload, payload_size)))
        << "Failed to load function configs from payload";

    config_entry_ = func_config_.find_by_func_id(func_id);
    CHECK(config_entry_ != nullptr) << "Invalid func_id " << func_id;
    initial_client_id_ = gsl::narrow_cast<uint16_t>(client_id);
}

EventDrivenWorker::~EventDrivenWorker() {}

void EventDrivenWorker::Start() {
    watch_fd_readable_cb_(message_pipe_fd_);
    NewFuncWorker(initial_client_id_);
}

void EventDrivenWorker::OnFdReadable(int fd) {
    if (fd == message_pipe_fd_) {
        OnMessagePipeReadable();
    } else if (func_worker_by_input_fd_.count(fd) > 0) {
        OnEnginePipeReadable(func_worker_by_input_fd_[fd]);
    } else if (outgoing_func_call_by_output_pipe_fd_.count(fd) > 0) {
        OnOutputPipeReadable(outgoing_func_call_by_output_pipe_fd_[fd]);
    } else {
        LOG(FATAL) << "Unknown fd " << fd;
    }
}

void EventDrivenWorker::OnFuncExecutionFinished(int64_t handle, bool success,
                                                std::span<const char> output) {
    FuncCall func_call = handle_to_func_call(handle);
    FuncWorkerState* worker_state = GetAssociatedFuncWorkerState(func_call);
    if (worker_state == nullptr) {
        LOG(ERROR) << "Invalid func call: " << FuncCallDebugString(func_call);
        return;
    }
    IncomingFuncCallState* func_call_state = incoming_func_calls_[func_call.full_call_id];
    incoming_func_calls_.erase(func_call.full_call_id);
    auto reclaim_func_call_state = gsl::finally([this, func_call_state] {
        incoming_func_call_pool_.Return(func_call_state);
    });
    int32_t processing_time = gsl::narrow_cast<int32_t>(
        GetMonotonicMicroTimestamp() - func_call_state->start_timestamp);
    VLOG(1) << "Finish executing func_call " << FuncCallDebugString(func_call);
    Message response;
    if (use_fifo_for_nested_call_) {
        worker_lib::FifoFuncCallFinished(
            func_call, success, output, processing_time, main_pipe_buf_, &response);
    } else {
        worker_lib::FuncCallFinished(
            func_call, success, output, processing_time, &response);
    }
    VLOG(1) << "Send response to engine";
    response.dispatch_delay = func_call_state->dispatch_delay;
    response.send_timestamp = GetMonotonicMicroTimestamp();
    PCHECK(io_utils::SendMessage(worker_state->output_pipe_fd, response));
}

bool EventDrivenWorker::NewOutgoingFuncCall(int64_t parent_handle, std::string_view func_name,
                                            std::span<const char> input, int64_t* handle) {
    const FuncConfig::Entry* func_entry = func_config_.find_by_func_name(func_name);
    if (func_entry == nullptr || func_entry->is_grpc_service) {
        LOG(ERROR) << "Function " << func_name << " does not exist";
        return false;
    }
    FuncCall parent_call = handle_to_func_call(parent_handle);
    FuncWorkerState* worker_state = GetAssociatedFuncWorkerState(parent_call);
    if (worker_state == nullptr) {
        LOG(ERROR) << "Invalid parent func call: " << FuncCallDebugString(parent_call);
        return false;
    }
    FuncCall func_call = NewFuncCall(
        gsl::narrow_cast<uint16_t>(func_entry->func_id),
        worker_state->client_id, worker_state->next_call_id++);
    *handle = func_call_to_handle(func_call);
    return NewOutgoingFuncCallCommon(parent_call, func_call, worker_state, input);
}

bool EventDrivenWorker::NewOutgoingGrpcCall(int64_t parent_handle, std::string_view service,
                                            std::string_view method, std::span<const char> request,
                                            int64_t* handle) {
    const FuncConfig::Entry* func_entry = func_config_.find_by_func_name(
        fmt::format("grpc:{}", service));
    if (func_entry == nullptr || !func_entry->is_grpc_service) {
        LOG(ERROR) << "gRPC service " << service << " does not exist";
        return false;
    }
    std::string method_str(method);
    if (func_entry->grpc_method_ids.count(method_str) == 0) {
        LOG(ERROR) << "gRPC service " << service << " does not have method " << method;
        return false;
    }
    int method_id = func_entry->grpc_method_ids.at(method_str);
    FuncCall parent_call = handle_to_func_call(parent_handle);
    FuncWorkerState* worker_state = GetAssociatedFuncWorkerState(parent_call);
    if (worker_state == nullptr) {
        LOG(ERROR) << "Invalid parent func call: " << FuncCallDebugString(parent_call);
        return false;
    }
    FuncCall func_call = NewFuncCallWithMethod(
        gsl::narrow_cast<uint16_t>(func_entry->func_id),
        gsl::narrow_cast<uint16_t>(method_id),
        worker_state->client_id, worker_state->next_call_id++);
    *handle = func_call_to_handle(func_call);
    return NewOutgoingFuncCallCommon(parent_call, func_call, worker_state, request);
}

EventDrivenWorker::FuncWorkerState* EventDrivenWorker::GetAssociatedFuncWorkerState(
        const FuncCall& incoming_func_call) {
    if (incoming_func_calls_.count(incoming_func_call.full_call_id) == 0) {
        LOG(ERROR) << "Cannot find func call: " << FuncCallDebugString(incoming_func_call);
        return nullptr;
    }
    IncomingFuncCallState* func_call_state = incoming_func_calls_[incoming_func_call.full_call_id];
    if (func_workers_.count(func_call_state->recv_client_id) == 0) {
        LOG(ERROR) << "Cannot find func worker with client_id " << func_call_state->recv_client_id;
        return nullptr;
    }
    return func_workers_[func_call_state->recv_client_id].get();
}

void EventDrivenWorker::NewFuncWorker(uint16_t client_id) {
    int engine_sock_fd = utils::UnixDomainSocketConnect(ipc::GetEngineUnixSocketPath());
    CHECK(engine_sock_fd != -1) << "Failed to connect to engine socket";
    int input_pipe_fd = ipc::FifoOpenForRead(ipc::GetFuncWorkerInputFifoName(client_id));
    Message message = NewFuncWorkerHandshakeMessage(
        gsl::narrow_cast<uint16_t>(config_entry_->func_id), client_id);
    PCHECK(io_utils::SendMessage(engine_sock_fd, message));
    Message response;
    CHECK(io_utils::RecvMessage(engine_sock_fd, &response, nullptr))
        << "Failed to receive handshake response from engine";
    CHECK(IsHandshakeResponseMessage(response))
        << "Receive invalid handshake response";
    if (response.flags & protocol::kUseFifoForNestedCallFlag) {
        if (!use_fifo_for_nested_call_) {
            LOG(INFO) << "Use extra FIFOs for handling nested call";
            use_fifo_for_nested_call_ = true;
        }
    }
    int output_pipe_fd = ipc::FifoOpenForWrite(ipc::GetFuncWorkerOutputFifoName(client_id));
    LOG(INFO) << "Handshake done: client_id=" << client_id;

    FuncWorkerState* worker_state = new FuncWorkerState;
    worker_state->client_id = client_id;
    worker_state->engine_sock_fd = engine_sock_fd;
    worker_state->input_pipe_fd = input_pipe_fd;
    worker_state->output_pipe_fd = output_pipe_fd;
    worker_state->next_call_id = 0;
    func_workers_[client_id] = std::unique_ptr<FuncWorkerState>(worker_state);
    func_worker_by_input_fd_[input_pipe_fd] = worker_state;

    watch_fd_readable_cb_(input_pipe_fd);
}

void EventDrivenWorker::ExecuteFunc(FuncWorkerState* worker_state,
                                    const Message& dispatch_func_call_message) {
    int32_t dispatch_delay = gsl::narrow_cast<int32_t>(
        GetMonotonicMicroTimestamp() - dispatch_func_call_message.send_timestamp);
    FuncCall func_call = GetFuncCallFromMessage(dispatch_func_call_message);
    VLOG(1) << "Execute func_call " << FuncCallDebugString(func_call);
    std::unique_ptr<ipc::ShmRegion> input_region;
    std::span<const char> input;
    if (!worker_lib::GetFuncCallInput(dispatch_func_call_message, &input, &input_region)) {
        Message response = NewFuncCallFailedMessage(func_call);
        response.send_timestamp = GetMonotonicMicroTimestamp();
        PCHECK(io_utils::SendMessage(worker_state->output_pipe_fd, response));
        return;
    }
    std::string method;
    if (is_grpc_service()) {
        if (func_call.method_id < config_entry_->grpc_methods.size()) {
            method = config_entry_->grpc_methods[func_call.method_id];
        } else {
            LOG(FATAL) << "Invalid method_id in func_call: " << FuncCallDebugString(func_call);
        }
    }
    IncomingFuncCallState* func_call_state = incoming_func_call_pool_.Get();
    func_call_state->func_call = func_call;
    func_call_state->recv_client_id = worker_state->client_id;
    func_call_state->dispatch_delay = dispatch_delay;
    func_call_state->start_timestamp = GetMonotonicMicroTimestamp();
    incoming_func_calls_[func_call.full_call_id] = func_call_state;
    incoming_func_call_cb_(func_call_to_handle(func_call), method, input);
}

bool EventDrivenWorker::NewOutgoingFuncCallCommon(const protocol::FuncCall& parent_call,
                                                  const protocol::FuncCall& func_call,
                                                  FuncWorkerState* worker_state,
                                                  std::span<const char> input) {
    VLOG(1) << "Invoke func_call " << FuncCallDebugString(func_call);
    Message invoke_func_message;
    std::unique_ptr<ipc::ShmRegion> input_region;
    if (!worker_lib::PrepareNewFuncCall(
            func_call, parent_call.full_call_id, input, &input_region, &invoke_func_message)) {
        return false;
    }

    int output_fifo = -1;
    if (use_fifo_for_nested_call_) {
        // Create fifo for output
        if (!ipc::FifoCreate(ipc::GetFuncCallOutputFifoName(func_call.full_call_id))) {
            LOG(ERROR) << "FifoCreate failed";
            return false;
        }
        output_fifo = ipc::FifoOpenForReadWrite(
            ipc::GetFuncCallOutputFifoName(func_call.full_call_id), /* nonblocking= */ true);
        if (output_fifo == -1) {
            LOG(ERROR) << "FifoOpenForReadWrite failed";
            ipc::FifoRemove(ipc::GetFuncCallOutputFifoName(func_call.full_call_id));
            return false;
        }
        watch_fd_readable_cb_(output_fifo);
    }

    OutgoingFuncCallState* func_call_state = outgoing_func_call_pool_.Get();
    func_call_state->func_call = func_call;
    func_call_state->input_region = std::move(input_region);
    func_call_state->output_pipe_fd = output_fifo;
    outgoing_func_calls_[func_call.full_call_id] = func_call_state;
    if (output_fifo != -1) {
        outgoing_func_call_by_output_pipe_fd_[output_fifo] = func_call_state;
    }

    invoke_func_message.send_timestamp = GetMonotonicMicroTimestamp();
    PCHECK(io_utils::SendMessage(worker_state->output_pipe_fd, invoke_func_message));
    VLOG(1) << "InvokeFuncMessage sent to engine";
    return true;
}

void EventDrivenWorker::OnMessagePipeReadable() {
    Message message;
    CHECK(io_utils::RecvMessage(message_pipe_fd_, &message, nullptr))
        << "Failed to receive message from launcher";
    if (IsCreateFuncWorkerMessage(message)) {
        NewFuncWorker(message.client_id);
    } else {
        LOG(FATAL) << "Unknown launcher message type";
    }
}

void EventDrivenWorker::OnEnginePipeReadable(FuncWorkerState* worker_state) {
    Message message;
    CHECK(io_utils::RecvMessage(worker_state->input_pipe_fd, &message, nullptr))
        << "Failed to receive message from engine";
    if (IsDispatchFuncCallMessage(message)) {
        ExecuteFunc(worker_state, message);
    } else if (IsFuncCallCompleteMessage(message) || IsFuncCallFailedMessage(message)) {
        if (use_fifo_for_nested_call_) {
            LOG(FATAL) << "UseFifoForNestedCall flag is set, should not receive this message";
        }
        FuncCall func_call = GetFuncCallFromMessage(message);
        if (outgoing_func_calls_.count(func_call.full_call_id) == 0) {
            LOG(ERROR) << "Unknown outgoing func call: " << FuncCallDebugString(func_call);
            return;
        }
        OnOutgoingFuncCallFinished(message, outgoing_func_calls_[func_call.full_call_id]);
    } else {
        LOG(FATAL) << "Unknown message type";
    }
}

void EventDrivenWorker::OnOutputPipeReadable(OutgoingFuncCallState* func_call_state) {
    outgoing_func_calls_.erase(func_call_state->func_call.full_call_id);
    int output_fifo = func_call_state->output_pipe_fd;
    outgoing_func_call_by_output_pipe_fd_.erase(output_fifo);
    stop_watch_fd_cb_(output_fifo);
    auto reclaim_func_call_state = gsl::finally([this, func_call_state] {
        func_call_state->input_region.reset(nullptr);
        outgoing_func_call_pool_.Return(func_call_state);
    });
    std::unique_ptr<ipc::ShmRegion> output_region;
    bool success = false;
    bool pipe_buffer_used = false;
    std::span<const char> output;
    if (worker_lib::FifoGetFuncCallOutput(
            func_call_state->func_call, output_fifo, main_pipe_buf_,
            &success, &output, &output_region, &pipe_buffer_used)) {
        outgoing_func_call_complete_cb_(func_call_to_handle(func_call_state->func_call),
                                        success, output);
    } else {
        LOG(ERROR) << "GetFuncCallOutput failed";
        outgoing_func_call_complete_cb_(func_call_to_handle(func_call_state->func_call),
                                        /* success= */ false,
                                        /* output= */ std::span<const char>());
    }
}

void EventDrivenWorker::OnOutgoingFuncCallFinished(const Message& message,
                                                   OutgoingFuncCallState* func_call_state) {
    outgoing_func_calls_.erase(func_call_state->func_call.full_call_id);
    auto reclaim_func_call_state = gsl::finally([this, func_call_state] {
        func_call_state->input_region.reset(nullptr);
        outgoing_func_call_pool_.Return(func_call_state);
    });
    if (IsFuncCallFailedMessage(message)) {
        outgoing_func_call_complete_cb_(func_call_to_handle(func_call_state->func_call),
                                        /* success= */ false,
                                        /* output= */ std::span<const char>());
        return;
    } else if (!IsFuncCallCompleteMessage(message)) {
        LOG(FATAL) << "Unknown message type";
    }
    std::unique_ptr<ipc::ShmRegion> output_region;
    if (message.payload_size < 0) {
        auto output_region = ipc::ShmOpen(
            ipc::GetFuncCallOutputShmName(func_call_state->func_call.full_call_id));
        if (output_region == nullptr) {
            LOG(ERROR) << "ShmOpen failed";
            outgoing_func_call_complete_cb_(func_call_to_handle(func_call_state->func_call),
                                            /* success= */ false,
                                            /* output= */ std::span<const char>());
            return;
        }
        output_region->EnableRemoveOnDestruction();
        if (output_region->size() != gsl::narrow_cast<size_t>(-message.payload_size)) {
            LOG(ERROR) << "Output size mismatch";
            outgoing_func_call_complete_cb_(func_call_to_handle(func_call_state->func_call),
                                            /* success= */ false,
                                            /* output= */ std::span<const char>());
            return;
        }
        outgoing_func_call_complete_cb_(func_call_to_handle(func_call_state->func_call),
                                        /* success= */ true, output_region->to_span());
    } else {
        std::span<const char> output = GetInlineDataFromMessage(message);
        outgoing_func_call_complete_cb_(func_call_to_handle(func_call_state->func_call),
                                        /* success= */ true, output);
    }
}

}  // namespace worker_lib
}  // namespace faas
