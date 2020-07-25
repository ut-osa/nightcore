#include "worker/v1/func_worker.h"

#include "common/time.h"
#include "ipc/base.h"
#include "ipc/fifo.h"
#include "utils/io.h"
#include "utils/socket.h"
#include "worker/worker_lib.h"

#include <fcntl.h>

namespace faas {
namespace worker_v1 {

using protocol::FuncCall;
using protocol::NewFuncCall;
using protocol::FuncCallDebugString;
using protocol::Message;
using protocol::GetFuncCallFromMessage;
using protocol::IsHandshakeResponseMessage;
using protocol::IsDispatchFuncCallMessage;
using protocol::IsFuncCallCompleteMessage;
using protocol::IsFuncCallFailedMessage;
using protocol::NewFuncWorkerHandshakeMessage;
using protocol::NewFuncCallFailedMessage;

FuncWorker::FuncWorker()
    : func_id_(-1), fprocess_id_(-1), client_id_(0), message_pipe_fd_(-1),
      use_engine_socket_(false), engine_tcp_port_(-1), use_fifo_for_nested_call_(false),
      func_call_timeout_(kDefaultFuncCallTimeout),
      engine_sock_fd_(-1), input_pipe_fd_(-1), output_pipe_fd_(-1),
      buffer_pool_for_pipes_("Pipes", PIPE_BUF), ongoing_invoke_func_(false),
      next_call_id_(0), current_func_call_id_(0) {}

FuncWorker::~FuncWorker() {
    if (engine_sock_fd_ != -1) {
        close(engine_sock_fd_);
    }
    if (input_pipe_fd_ != -1 && !use_engine_socket_) {
        close(input_pipe_fd_);
    }
    if (output_pipe_fd_ != -1 && !use_engine_socket_) {
        close(output_pipe_fd_);
    }
}

void FuncWorker::Serve() {
    CHECK(func_id_ != -1);
    CHECK(fprocess_id_ != -1);
    CHECK(client_id_ > 0);
    LOG(INFO) << "My client_id is " << client_id_;
    // Load function library
    CHECK(!func_library_path_.empty());
    func_library_ = utils::DynamicLibrary::Create(func_library_path_);
    init_fn_ = func_library_->LoadSymbol<faas_init_fn_t>("faas_init");
    create_func_worker_fn_ = func_library_->LoadSymbol<faas_create_func_worker_fn_t>(
        "faas_create_func_worker");
    destroy_func_worker_fn_ = func_library_->LoadSymbol<faas_destroy_func_worker_fn_t>(
        "faas_destroy_func_worker");
    func_call_fn_ = func_library_->LoadSymbol<faas_func_call_fn_t>(
        "faas_func_call");
    CHECK(init_fn_() == 0) << "Failed to initialize loaded library";
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
    // Connect to engine via IPC path
    if (engine_tcp_port_ == -1) {
        engine_sock_fd_ = utils::UnixDomainSocketConnect(ipc::GetEngineUnixSocketPath());
    } else {
        std::string host(utils::GetEnvVariable("FAAS_ENGINE_HOST", "127.0.0.1"));
        engine_sock_fd_ = utils::TcpSocketConnect(host.c_str(), engine_tcp_port_);
    }
    CHECK(engine_sock_fd_ != -1) << "Failed to connect to engine socket";
    HandshakeWithEngine();
    // Enter main serving loop
    MainServingLoop();
}

void FuncWorker::MainServingLoop() {
    CHECK(create_func_worker_fn_(this,
                                 &FuncWorker::InvokeFuncWrapper,
                                 &FuncWorker::AppendOutputWrapper,
                                 &worker_handle_) == 0)
        << "Failed to create function worker";

    if (!use_engine_socket_) {
        ipc::FifoUnsetNonblocking(input_pipe_fd_);
    }

    while (true) {
        Message message;
        CHECK(io_utils::RecvMessage(input_pipe_fd_, &message, nullptr))
            << "Failed to receive message from engine";
        if (IsDispatchFuncCallMessage(message)) {
            ExecuteFunc(message);
        } else {
            LOG(FATAL) << "Unknown message type";
        }
    }

    CHECK(destroy_func_worker_fn_(worker_handle_) == 0)
        << "Failed to destroy function worker";
}

void FuncWorker::HandshakeWithEngine() {
    if (use_engine_socket_) {
        LOG(INFO) << "Use engine socket for messages";
        input_pipe_fd_ = engine_sock_fd_;
    } else {
        LOG(INFO) << "Use extra pipes for messages";
        input_pipe_fd_ = ipc::FifoOpenForRead(ipc::GetFuncWorkerInputFifoName(client_id_));
    }
    Message message = NewFuncWorkerHandshakeMessage(func_id_, client_id_);
    PCHECK(io_utils::SendMessage(engine_sock_fd_, message));
    Message response;
    CHECK(io_utils::RecvMessage(engine_sock_fd_, &response, nullptr))
        << "Failed to receive handshake response from engine";
    CHECK(IsHandshakeResponseMessage(response))
        << "Receive invalid handshake response";
    if (use_engine_socket_) {
        output_pipe_fd_ = engine_sock_fd_;
    } else {
        output_pipe_fd_ = ipc::FifoOpenForWrite(ipc::GetFuncWorkerOutputFifoName(client_id_));
    }
    if (response.flags & protocol::kUseFifoForNestedCallFlag) {
        LOG(INFO) << "Use extra FIFOs for handling nested call";
        use_fifo_for_nested_call_ = true;
    }
    LOG(INFO) << "Handshake done";
}

void FuncWorker::ExecuteFunc(const Message& dispatch_func_call_message) {
    int32_t dispatch_delay = gsl::narrow_cast<int32_t>(
        GetMonotonicMicroTimestamp() - dispatch_func_call_message.send_timestamp);
    FuncCall func_call = GetFuncCallFromMessage(dispatch_func_call_message);
    VLOG(1) << "Execute func_call " << FuncCallDebugString(func_call);
    std::unique_ptr<ipc::ShmRegion> input_region;
    std::span<const char> input;
    if (!worker_lib::GetFuncCallInput(dispatch_func_call_message, &input, &input_region)) {
        Message response = NewFuncCallFailedMessage(func_call);
        response.send_timestamp = GetMonotonicMicroTimestamp();
        PCHECK(io_utils::SendMessage(output_pipe_fd_, response));
        return;
    }
    func_output_buffer_.Reset();
    current_func_call_id_.store(func_call.full_call_id);
    int64_t start_timestamp = GetMonotonicMicroTimestamp();
    int ret = func_call_fn_(worker_handle_, input.data(), input.size());
    int32_t processing_time = gsl::narrow_cast<int32_t>(
        GetMonotonicMicroTimestamp() - start_timestamp);
    ReclaimInvokeFuncResources();
    VLOG(1) << "Finish executing func_call " << FuncCallDebugString(func_call);
    Message response;
    if (use_fifo_for_nested_call_) {
        worker_lib::FifoFuncCallFinished(
            func_call, /* success= */ ret == 0, func_output_buffer_.to_span(),
            processing_time, main_pipe_buf_, &response);
    } else {
        worker_lib::FuncCallFinished(
            func_call, /* success= */ ret == 0, func_output_buffer_.to_span(),
            processing_time, &response);
    }
    VLOG(1) << "Send response to engine";
    response.dispatch_delay = dispatch_delay;
    response.send_timestamp = GetMonotonicMicroTimestamp();
    PCHECK(io_utils::SendMessage(output_pipe_fd_, response));
}

bool FuncWorker::InvokeFunc(const char* func_name, const char* input_data, size_t input_length,
                            const char** output_data, size_t* output_length) {
    const FuncConfig::Entry* func_entry = func_config_.find_by_func_name(
        std::string_view(func_name, strlen(func_name)));
    if (func_entry == nullptr) {
        LOG(ERROR) << "Function " << func_name << " does not exist";
        return false;
    }
    FuncCall func_call = NewFuncCall(
        gsl::narrow_cast<uint16_t>(func_entry->func_id),
        client_id_, next_call_id_.fetch_add(1));
    VLOG(1) << "Invoke func_call " << FuncCallDebugString(func_call);
    Message invoke_func_message;
    std::unique_ptr<ipc::ShmRegion> input_region;
    if (!worker_lib::PrepareNewFuncCall(
            func_call, /* parent_func_call= */ current_func_call_id_.load(),
            std::span<const char>(input_data, input_length),
            &input_region, &invoke_func_message)) {
        return false;
    }
    if (use_fifo_for_nested_call_) {
        return FifoWaitInvokeFunc(&invoke_func_message, output_data, output_length);
    } else {
        return WaitInvokeFunc(&invoke_func_message, output_data, output_length);
    }
}

bool FuncWorker::WaitInvokeFunc(Message* invoke_func_message,
                                const char** output_data, size_t* output_length) {
    FuncCall func_call = GetFuncCallFromMessage(*invoke_func_message);
    // Send message to engine (dispatcher)
    {
        absl::MutexLock lk(&mu_);
        if (ongoing_invoke_func_) {
            // TODO: fix this
            LOG(FATAL) << "NaiveWaitInvokeFunc cannot execute concurrently";
        }
        ongoing_invoke_func_ = true;
        invoke_func_message->send_timestamp = GetMonotonicMicroTimestamp();
        PCHECK(io_utils::SendMessage(output_pipe_fd_, *invoke_func_message));
    }
    VLOG(1) << "InvokeFuncMessage sent to engine";
    Message result_message;
    CHECK(io_utils::RecvMessage(input_pipe_fd_, &result_message, nullptr));
    if (IsFuncCallFailedMessage(result_message)) {
        absl::MutexLock lk(&mu_);
        ongoing_invoke_func_ = false;
        return false;
    } else if (!IsFuncCallCompleteMessage(result_message)) {
        LOG(FATAL) << "Unknown message type";
    }
    InvokeFuncResource invoke_func_resource = {
        .func_call = func_call,
        .output_region = nullptr,
        .pipe_buffer = nullptr
    };
    if (result_message.payload_size < 0) {
        auto output_region = ipc::ShmOpen(
            ipc::GetFuncCallOutputShmName(func_call.full_call_id));
        if (output_region == nullptr) {
            LOG(ERROR) << "ShmOpen failed";
            return false;
        }
        output_region->EnableRemoveOnDestruction();
        if (output_region->size() != gsl::narrow_cast<size_t>(-result_message.payload_size)) {
            LOG(ERROR) << "Output size mismatch";
            return false;
        }
        *output_data = output_region->base();
        *output_length = output_region->size();
        invoke_func_resource.output_region = std::move(output_region);
        absl::MutexLock lk(&mu_);
        invoke_func_resources_.push_back(std::move(invoke_func_resource));
        ongoing_invoke_func_ = false;
    } else {
        absl::MutexLock lk(&mu_);
        char* buffer;
        size_t size;
        buffer_pool_for_pipes_.Get(&buffer, &size);
        CHECK(size >= sizeof(Message));
        memcpy(buffer, &result_message, sizeof(Message));
        Message* message_copy = reinterpret_cast<Message*>(buffer);
        std::span<const char> output = GetInlineDataFromMessage(*message_copy);
        invoke_func_resource.pipe_buffer = buffer;
        *output_data = output.data();
        *output_length = output.size();
        invoke_func_resources_.push_back(std::move(invoke_func_resource));
        ongoing_invoke_func_ = false;
    }
    return true;
}

bool FuncWorker::FifoWaitInvokeFunc(Message* invoke_func_message,
                                    const char** output_data, size_t* output_length) {
    FuncCall func_call = GetFuncCallFromMessage(*invoke_func_message);
    // Create fifo for output
    if (!ipc::FifoCreate(ipc::GetFuncCallOutputFifoName(func_call.full_call_id))) {
        LOG(ERROR) << "FifoCreate failed";
        return false;
    }
    auto remove_output_fifo = gsl::finally([func_call] {
        ipc::FifoRemove(ipc::GetFuncCallOutputFifoName(func_call.full_call_id));
    });
    int output_fifo = ipc::FifoOpenForReadWrite(
        ipc::GetFuncCallOutputFifoName(func_call.full_call_id), /* nonblocking= */ true);
    if (output_fifo == -1) {
        LOG(ERROR) << "FifoOpenForReadWrite failed";
        return false;
    }
    auto close_output_fifo = gsl::finally([output_fifo] {
        if (close(output_fifo) != 0) {
            PLOG(ERROR) << "close failed";
        }
    });
    // Send message to engine (dispatcher)
    {
        absl::MutexLock lk(&mu_);
        invoke_func_message->send_timestamp = GetMonotonicMicroTimestamp();
        PCHECK(io_utils::SendMessage(output_pipe_fd_, *invoke_func_message));
    }
    VLOG(1) << "InvokeFuncMessage sent to engine";
    int timeout_ms = -1;
    if (func_call_timeout_ != absl::InfiniteDuration()) {
        timeout_ms = gsl::narrow_cast<int>(absl::ToInt64Milliseconds(func_call_timeout_));
    }
    if (!ipc::FifoPollForRead(output_fifo, timeout_ms)) {
        LOG(ERROR) << "FifoPollForRead failed";
        return false;
    }
    char* pipe_buffer;
    {
        absl::MutexLock lk(&mu_);
        size_t size;
        buffer_pool_for_pipes_.Get(&pipe_buffer, &size);
        DCHECK(size == PIPE_BUF);
    }
    std::unique_ptr<ipc::ShmRegion> output_region;
    bool success = false;
    bool pipe_buffer_used = false;
    std::span<const char> output;
    if (worker_lib::FifoGetFuncCallOutput(
            func_call, output_fifo, pipe_buffer,
            &success, &output, &output_region, &pipe_buffer_used)) {
        absl::MutexLock lk(&mu_);
        InvokeFuncResource invoke_func_resource = {
            .func_call = func_call,
            .output_region = nullptr,
            .pipe_buffer = nullptr
        };
        if (pipe_buffer_used) {
            invoke_func_resource.pipe_buffer = pipe_buffer;
        } else {
            buffer_pool_for_pipes_.Return(pipe_buffer);
        }
        if (output_region != nullptr) {
            invoke_func_resource.output_region = std::move(output_region);
        }
        invoke_func_resources_.push_back(std::move(invoke_func_resource));
        if (success) {
            *output_data = output.data();
            *output_length = output.size();
            return true;
        } else {
            return false;
        }
    } else {
        absl::MutexLock lk(&mu_);
        buffer_pool_for_pipes_.Return(pipe_buffer);
        return false;
    }
}

void FuncWorker::ReclaimInvokeFuncResources() {
    absl::MutexLock lk(&mu_);
    for (const auto& resource : invoke_func_resources_) {
        if (resource.pipe_buffer != nullptr) {
            buffer_pool_for_pipes_.Return(resource.pipe_buffer);
        }
    }
    invoke_func_resources_.clear();
}

void FuncWorker::AppendOutputWrapper(void* caller_context, const char* data, size_t length) {
    FuncWorker* self = reinterpret_cast<FuncWorker*>(caller_context);
    self->func_output_buffer_.AppendData(data, length);
}

int FuncWorker::InvokeFuncWrapper(void* caller_context, const char* func_name,
                                  const char* input_data, size_t input_length,
                                  const char** output_data, size_t* output_length) {
    *output_data = nullptr;
    *output_length = 0;
    FuncWorker* self = reinterpret_cast<FuncWorker*>(caller_context);
    bool success = self->InvokeFunc(func_name, input_data, input_length,
                                    output_data, output_length);
    return success ? 0 : -1;
}

}  // namespace worker_v1
}  // namespace faas
