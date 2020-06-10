#include "worker/v1/func_worker.h"

#include "common/time.h"
#include "ipc/base.h"
#include "ipc/fifo.h"
#include "utils/io.h"
#include "utils/socket.h"

#include <fcntl.h>

namespace faas {
namespace worker_v1 {

using protocol::FuncCall;
using protocol::NewFuncCall;
using protocol::FuncCallDebugString;
using protocol::Message;
using protocol::GetFuncCallFromMessage;
using protocol::IsHandshakeResponseMessage;
using protocol::IsInvokeFuncMessage;
using protocol::NewFuncWorkerHandshakeMessage;
using protocol::NewFuncCallCompleteMessage;
using protocol::NewFuncCallFailedMessage;
using protocol::NewInvokeFuncMessage;

constexpr absl::Duration FuncWorker::kDefaultFuncCallTimeout;

FuncWorker::FuncWorker()
    : func_id_(-1), fprocess_id_(-1), client_id_(0), func_call_timeout_(kDefaultFuncCallTimeout),
      gateway_sock_fd_(-1), input_pipe_fd_(-1), output_pipe_fd_(-1),
      buffer_pool_for_pipes_("Pipes", PIPE_BUF), next_call_id_(0),
      gateway_message_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("gateway_message_delay")),
      processing_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("processing_delay")),
      input_size_stat_(
          stat::StatisticsCollector<uint32_t>::StandardReportCallback("input_size")),
      output_size_stat_(
          stat::StatisticsCollector<uint32_t>::StandardReportCallback("output_size")) {}

FuncWorker::~FuncWorker() {
    close(gateway_sock_fd_);
    close(input_pipe_fd_);
    close(output_pipe_fd_);
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
    // Connect to gateway via IPC path
    gateway_sock_fd_ = utils::UnixDomainSocketConnect(ipc::GetGatewayUnixSocketPath());
    HandshakeWithGateway();
    // Enter main serving loop
    MainServingLoop();
}

void FuncWorker::MainServingLoop() {
    void* func_worker;
    CHECK(create_func_worker_fn_(this,
                                 &FuncWorker::InvokeFuncWrapper,
                                 &FuncWorker::AppendOutputWrapper,
                                 &func_worker) == 0)
        << "Failed to create function worker";

    ipc::FifoUnsetNonblocking(input_pipe_fd_);

    while (true) {
        Message message;
        CHECK(io_utils::RecvMessage(input_pipe_fd_, &message, nullptr))
            << "Failed to receive message from gateway";
#ifdef __FAAS_ENABLE_PROFILING
        gateway_message_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(
            GetMonotonicMicroTimestamp() - message.send_timestamp));
#endif
        if (IsInvokeFuncMessage(message)) {
            FuncCall func_call = GetFuncCallFromMessage(message);
            ExecuteFunc(func_worker, func_call);
        } else {
            LOG(FATAL) << "Unknown message type";
        }
    }

    CHECK(destroy_func_worker_fn_(func_worker) == 0)
        << "Failed to destroy function worker";
}

void FuncWorker::HandshakeWithGateway() {
    input_pipe_fd_ = ipc::FifoOpenForRead(ipc::GetFuncWorkerInputFifoName(client_id_));
    Message message = NewFuncWorkerHandshakeMessage(func_id_, client_id_);
    PCHECK(io_utils::SendMessage(gateway_sock_fd_, message));
    Message response;
    CHECK(io_utils::RecvMessage(gateway_sock_fd_, &response, nullptr))
        << "Failed to receive handshake response from gateway";
    size_t payload_size = response.payload_size;
    char* payload = new char[payload_size];
    auto reclaim_payload_buffer = gsl::finally([payload] { delete[] payload; });
    CHECK(io_utils::RecvData(gateway_sock_fd_, payload, payload_size, nullptr))
        << "Failed to receive payload data from gateway";
    func_config_.Load(std::string_view(payload, payload_size));
    output_pipe_fd_ = ipc::FifoOpenForWrite(ipc::GetFuncWorkerOutputFifoName(client_id_));
    LOG(INFO) << "Handshake done";
}

void FuncWorker::ExecuteFunc(void* worker_handle, const FuncCall& func_call) {
    VLOG(1) << "Execute func_call " << FuncCallDebugString(func_call);
    auto input_region = ipc::ShmOpen(ipc::GetFuncCallInputShmName(func_call.full_call_id));
    input_size_stat_.AddSample(input_region->size());
    func_output_buffer_.Reset();
    int64_t start_timestamp = GetMonotonicMicroTimestamp();
    int ret = func_call_fn_(
        worker_handle, input_region->base(), input_region->size());
    int32_t processing_time = gsl::narrow_cast<int32_t>(
        GetMonotonicMicroTimestamp() - start_timestamp);
    processing_delay_stat_.AddSample(processing_time);
    if (ret == 0) {
        output_size_stat_.AddSample(func_output_buffer_.length());
    }
    ReclaimInvokeFuncResources();
    VLOG(1) << "Finish executing func_call " << FuncCallDebugString(func_call);
    Message response;
    if (func_call.client_id == 0) {
        // FuncCall from gateway, will use shm for output
        if (ret == 0) {
            if (WriteOutputToShm(func_call)) {
                response = NewFuncCallCompleteMessage(func_call);
            } else {
                response = NewFuncCallFailedMessage(func_call);
            }
        } else {
            response = NewFuncCallFailedMessage(func_call);
        }
    } else {
        // FuncCall from other FuncWorker, will use fifo for output
        if (WriteOutputToFifo(func_call, ret == 0)) {
            response = NewFuncCallCompleteMessage(func_call);
        } else {
            response = NewFuncCallFailedMessage(func_call);
        }
    }
    VLOG(1) << "Send response to gateway";
#ifdef __FAAS_ENABLE_PROFILING
    response.send_timestamp = GetMonotonicMicroTimestamp();
    response.processing_time = processing_time;
#endif
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
    // Create shm for input
    auto input_region = ipc::ShmCreate(
        ipc::GetFuncCallInputShmName(func_call.full_call_id), input_length);
    if (input_region == nullptr) {
        LOG(ERROR) << "ShmCreate failed";
        return false;
    }
    input_region->EnableRemoveOnDestruction();
    if (input_length > 0) {
        memcpy(input_region->base(), input_data, input_length);
    }
    // Create fifo for output
    if (!ipc::FifoCreate(ipc::GetFuncCallOutputFifoName(func_call.full_call_id))) {
        LOG(ERROR) << "FifoCreate failed";
        return false;
    }
    auto remove_output_fifo = gsl::finally([func_call] {
        ipc::FifoRemove(ipc::GetFuncCallOutputFifoName(func_call.full_call_id));
    });
    int output_fifo = ipc::FifoOpenForRead(
        ipc::GetFuncCallOutputFifoName(func_call.full_call_id), /* nonblocking= */ true);
    if (output_fifo == -1) {
        LOG(ERROR) << "FifoOpenForRead failed";
        return false;
    }
    auto close_output_fifo = gsl::finally([output_fifo] {
        PCHECK(close(output_fifo) == 0) << "close failed";
    });
    // Send message to gateway (dispatcher)
    Message message = NewInvokeFuncMessage(func_call);
#ifdef __FAAS_ENABLE_PROFILING
    message.send_timestamp = GetMonotonicMicroTimestamp();
    message.processing_time = 0;
#endif
    {
        absl::MutexLock lk(&mu_);
        PCHECK(io_utils::SendMessage(output_pipe_fd_, message));
    }
    VLOG(1) << "InvokeFuncMessage sent to gateway";
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
    InvokeFuncResource invoke_func_resource = {
        .func_call = func_call,
        .output_region = nullptr,
        .pipe_buffer = pipe_buffer
    };
    if (ReadOutputFromFifo(output_fifo, &invoke_func_resource, output_data, output_length)) {
        absl::MutexLock lk(&mu_);
        if (invoke_func_resource.pipe_buffer == nullptr) {
            buffer_pool_for_pipes_.Return(pipe_buffer);
        }
        invoke_func_resources_.push_back(std::move(invoke_func_resource));
        return true;
    } else {
        absl::MutexLock lk(&mu_);
        buffer_pool_for_pipes_.Return(pipe_buffer);
        return false;
    }
}

bool FuncWorker::WriteOutputToShm(const protocol::FuncCall& func_call) {
    auto output_region = ipc::ShmCreate(
        ipc::GetFuncCallOutputShmName(func_call.full_call_id), func_output_buffer_.length());
    if (output_region == nullptr) {
        LOG(ERROR) << "ShmCreate failed";
        return false;
    }
    if (func_output_buffer_.length() > 0) {
        memcpy(output_region->base(), func_output_buffer_.data(),
               func_output_buffer_.length());
    }
    return true;
}

bool FuncWorker::WriteOutputToFifo(const protocol::FuncCall& func_call, bool func_call_succeed) {
    VLOG(1) << "Start writing output to FIFO";
    char* pipe_buffer;
    {
        absl::MutexLock lk(&mu_);
        size_t size;
        buffer_pool_for_pipes_.Get(&pipe_buffer, &size);
        DCHECK(size == PIPE_BUF);
    }
    auto reclaim_pipe_buffer = gsl::finally([this, pipe_buffer] {
        absl::MutexLock lk(&mu_);
        buffer_pool_for_pipes_.Return(pipe_buffer);
    });
    int output_fifo = ipc::FifoOpenForWrite(
        ipc::GetFuncCallOutputFifoName(func_call.full_call_id), /* nonblocking= */ true);
    if (output_fifo == -1) {
        LOG(ERROR) << "FifoOpenForWrite failed";
        return false;
    }
    auto close_output_fifo = gsl::finally([output_fifo] {
        PCHECK(close(output_fifo) == 0) << "close failed";
    });
    int32_t header = func_call_succeed ? func_output_buffer_.length() : -1;
    size_t write_size = sizeof(int32_t);
    memcpy(pipe_buffer, &header, sizeof(int32_t));
    if (func_call_succeed) {
        if (func_output_buffer_.length() + sizeof(int32_t) <= PIPE_BUF) {
            write_size += func_output_buffer_.length();
            DCHECK(write_size <= PIPE_BUF);
            memcpy(pipe_buffer + sizeof(uint32_t), func_output_buffer_.data(),
                   func_output_buffer_.length());
        } else {
            WriteOutputToShm(func_call);
        }
    }
    ssize_t nwrite = write(output_fifo, pipe_buffer, write_size);
    if (nwrite < 0) {
        PLOG(ERROR) << "Failed to write to output fifo";
        return false;
    }
    if (gsl::narrow_cast<size_t>(nwrite) < write_size) {
        LOG(ERROR) << "Writing " << write_size << " bytes to output fifo is not atomic";
        return false;
    }
    return true;
}

bool FuncWorker::ReadOutputFromFifo(int fd, InvokeFuncResource* invoke_func_resource,
                                    const char** output_data, size_t* output_length) {
    char* pipe_buffer = invoke_func_resource->pipe_buffer;
    invoke_func_resource->pipe_buffer = nullptr;
    ssize_t nread = read(fd, pipe_buffer, PIPE_BUF);
    if (nread < 0) {
        PLOG(ERROR) << "Failed to read from fifo";
        return false;
    }
    if (gsl::narrow_cast<size_t>(nread) < sizeof(int32_t)) {
        LOG(ERROR) << "Cannot read header from fifo";
        return false;
    }
    int32_t header;
    memcpy(&header, pipe_buffer, sizeof(int32_t));
    if (header < 0) {
        return false;
    }
    size_t output_size = gsl::narrow_cast<size_t>(header);
    if (sizeof(int32_t) + output_size <= PIPE_BUF) {
        if (gsl::narrow_cast<size_t>(nread) < sizeof(int32_t) + output_size) {
            LOG(ERROR) << "Not all fifo data is read?";
            return false;
        }
        *output_data = pipe_buffer + sizeof(int32_t);
        *output_length = output_size;
        invoke_func_resource->pipe_buffer = pipe_buffer;
    } else if (gsl::narrow_cast<size_t>(nread) == sizeof(int32_t)) {
        auto output_region = ipc::ShmOpen(
            ipc::GetFuncCallOutputShmName(invoke_func_resource->func_call.full_call_id));
        if (output_region == nullptr) {
            LOG(ERROR) << "ShmOpen failed";
            return false;
        }
        if (output_region->size() != output_size) {
            LOG(ERROR) << "Output size mismatch";
            return false;
        }
        *output_data = output_region->base();
        *output_length = output_region->size();
        invoke_func_resource->output_region = std::move(output_region);
    } else {
        LOG(ERROR) << "Invalid data read from output fifo";
        return false;
    }
    return true;
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
