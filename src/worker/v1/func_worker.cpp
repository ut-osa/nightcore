#include "worker/v1/func_worker.h"

#include "common/time.h"
#include "ipc/base.h"
#include "ipc/fifo.h"
#include "ipc/shm_region.h"
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
    : func_id_(-1), fprocess_id_(-1), client_id_(0),
      gateway_sock_fd_(-1), input_pipe_fd_(-1), output_pipe_fd_(-1), next_call_id_(0),
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

    // Unset O_NONBLOCK for input_pipe_fd_
    int flags = fcntl(input_pipe_fd_, F_GETFL, 0);
    PCHECK(flags != -1) << "fcntl failed";
    PCHECK(fcntl(input_pipe_fd_, F_SETFL, flags & ~O_NONBLOCK) == 0) << "fcntl failed";

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
    {
        absl::MutexLock lk(&mu_);
        DCHECK(temporary_buffers_.empty());
    }
    int64_t start_timestamp = GetMonotonicMicroTimestamp();
    int ret = func_call_fn_(
        worker_handle, input_region->base(), input_region->size());
    int32_t processing_time = gsl::narrow_cast<int32_t>(
        GetMonotonicMicroTimestamp() - start_timestamp);
    processing_delay_stat_.AddSample(processing_time);
    {
        absl::MutexLock lk(&mu_);
        for (char* buffer : temporary_buffers_) {
            delete[] buffer;
        }
        temporary_buffers_.clear();
    }
    Message response;
    if (ret == 0) {
        response = NewFuncCallCompleteMessage(func_call);
    } else {
        response = NewFuncCallFailedMessage(func_call);
    }
#ifdef __FAAS_ENABLE_PROFILING
    response.send_timestamp = GetMonotonicMicroTimestamp();
    response.processing_time = processing_time;
#endif
    VLOG(1) << "Finish executing func_call " << FuncCallDebugString(func_call);
    if (ret == 0 && func_call.client_id == 0) {
        // FuncCall from gateway, will use shm for output
        output_size_stat_.AddSample(func_output_buffer_.length());
        auto output_region = ipc::ShmCreate(
            ipc::GetFuncCallOutputShmName(func_call.full_call_id), func_output_buffer_.length());
        if (func_output_buffer_.length() > 0) {
            memcpy(output_region->base(), func_output_buffer_.data(), func_output_buffer_.length());
        }
    }
    VLOG(1) << "Send response to gateway";
    PCHECK(io_utils::SendMessage(output_pipe_fd_, response));
    if (func_call.client_id == 0) {
        // We're done if FuncCall from gateway
        return;
    }
    // Write output to FIFO
    VLOG(1) << "Start writing output to FIFO";
    int output_fifo = ipc::FifoOpenForWrite(
        ipc::GetFuncCallOutputFifoName(func_call.full_call_id), /* nonblocking= */ false);
    if (ret == 0) {
        output_size_stat_.AddSample(func_output_buffer_.length());
        int32_t output_size = func_output_buffer_.length();
        PCHECK(io_utils::SendMessage(output_fifo, output_size));
        PCHECK(io_utils::SendData(output_fifo, func_output_buffer_.to_span()));
    } else {
        int32_t err_ret = -1;
        PCHECK(io_utils::SendMessage(output_fifo, err_ret));
    }
    VLOG(1) << "Finish writing output to FIFO";
    PCHECK(close(output_fifo) == 0) << "close failed";
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
    {
        absl::MutexLock lk(&mu_);
        PCHECK(io_utils::SendMessage(output_pipe_fd_, message));
    }
    VLOG(1) << "InvokeFuncMessage sent to gateway";
    int32_t ret;
    PCHECK(io_utils::RecvMessage(output_fifo, &ret, nullptr));
    if (ret >= 0) {
        *output_length = ret;
        if (ret > 0) {
            char* buffer = new char[*output_length];
            PCHECK(io_utils::RecvData(output_fifo, buffer, *output_length, nullptr));
            *output_data = buffer;
            {
                absl::MutexLock lk(&mu_);
                temporary_buffers_.insert(buffer);
            }
        } else {
            *output_data = nullptr;
        }
    } else {
        *output_data = nullptr;
        *output_length = 0;
        return false;
    }
    return true;
}

void FuncWorker::AppendOutputWrapper(void* caller_context, const char* data, size_t length) {
    FuncWorker* self = reinterpret_cast<FuncWorker*>(caller_context);
    self->func_output_buffer_.AppendData(data, length);
}

int FuncWorker::InvokeFuncWrapper(void* caller_context, const char* func_name,
                                  const char* input_data, size_t input_length,
                                  const char** output_data, size_t* output_length) {
    FuncWorker* self = reinterpret_cast<FuncWorker*>(caller_context);
    bool success = self->InvokeFunc(func_name, input_data, input_length,
                                    output_data, output_length);
    return success ? 0 : -1;
}

}  // namespace worker_v1
}  // namespace faas
