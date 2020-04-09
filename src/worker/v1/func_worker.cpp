#include "worker/v1/func_worker.h"

#include "common/time.h"
#include "common/protocol.h"
#include "utils/io.h"
#include "utils/socket.h"

namespace faas {
namespace worker_v1 {

using protocol::FuncCall;
using protocol::MessageType;
using protocol::Message;
using protocol::Role;
using protocol::Status;
using protocol::HandshakeMessage;
using protocol::HandshakeResponse;

FuncWorker::FuncWorker()
    : func_id_(-1), input_pipe_fd_(-1), output_pipe_fd_(-1),
      gateway_sock_fd_(-1), gateway_disconnected_(false),
      gateway_ipc_thread_("GatewayIpc", absl::bind_front(&FuncWorker::GatewayIpcThreadMain, this)),
      next_call_id_(0),
      gateway_message_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("gateway_message_delay")),
      watchdog_message_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("watchdog_message_delay")),
      processing_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("processing_delay")),
      system_protocol_overhead_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("system_protocol_overhead")),
      input_size_stat_(
          stat::StatisticsCollector<uint32_t>::StandardReportCallback("input_size")),
      output_size_stat_(
          stat::StatisticsCollector<uint32_t>::StandardReportCallback("output_size")),
      incoming_requests_counter_(
          stat::Counter::StandardReportCallback("incoming_ruquests")) {}

FuncWorker::~FuncWorker() {
    close(gateway_sock_fd_);
    gateway_ipc_thread_.Join();
}

void FuncWorker::Serve() {
    // Load function config file
    CHECK(!func_config_file_.empty());
    CHECK(func_config_.Load(func_config_file_));
    CHECK(func_id_ != -1);
    CHECK(func_config_.find_by_func_id(func_id_) != nullptr);
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
    // Ensure we know pipe fds to the watchdog
    CHECK(input_pipe_fd_ != -1);
    CHECK(output_pipe_fd_ != -1);
    // Create shared memory pool
    CHECK(!shared_mem_path_.empty());
    shared_memory_ = std::make_unique<utils::SharedMemory>(shared_mem_path_);
    // Connect to gateway via IPC path
    CHECK(!gateway_ipc_path_.empty());
    gateway_sock_fd_ = utils::UnixDomainSocketConnect(gateway_ipc_path_);
    GatewayIpcHandshake();
    gateway_ipc_thread_.Start();
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

    while (true) {
        Message message;
        bool input_pipe_closed;
        if (!io_utils::RecvMessage(input_pipe_fd_, &message, &input_pipe_closed)) {
            if (input_pipe_closed) {
                LOG(WARNING) << "Pipe to watchdog closed remotely";
                break;
            } else {
                PLOG(FATAL) << "Failed to read from watchdog pipe";
            }
        }
#ifdef __FAAS_ENABLE_PROFILING
        watchdog_message_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(
            GetMonotonicMicroTimestamp() - message.send_timestamp));
#endif
        MessageType type{message.message_type};
        if (type == MessageType::INVOKE_FUNC) {
            incoming_requests_counter_.Tick();
            Message response;
            response.func_call = message.func_call;
            int64_t start_timestamp = GetMonotonicMicroTimestamp();
            bool success = RunFuncHandler(func_worker, message.func_call.full_call_id);
            int32_t processing_time = gsl::narrow_cast<int32_t>(
                GetMonotonicMicroTimestamp() - start_timestamp);
            processing_delay_stat_.AddSample(processing_time);
            if (success) {
                response.message_type = gsl::narrow_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
            } else {
                response.message_type = gsl::narrow_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
            }
#ifdef __FAAS_ENABLE_PROFILING
            response.send_timestamp = GetMonotonicMicroTimestamp();
            response.processing_time = processing_time;
#endif
            if (success) {
                PCHECK(io_utils::SendMessage(gateway_sock_fd_, response));
            }
            PCHECK(io_utils::SendMessage(output_pipe_fd_, response));
        } else {
            LOG(FATAL) << "Unknown message type";
        }
    }

    CHECK(destroy_func_worker_fn_(func_worker) == 0)
        << "Failed to destroy function worker";
}

void FuncWorker::GatewayIpcHandshake() {
    HandshakeMessage message = {
        .role = gsl::narrow_cast<uint16_t>(Role::FUNC_WORKER),
        .func_id = gsl::narrow_cast<uint16_t>(func_id_)
    };
    PCHECK(io_utils::SendMessage(gateway_sock_fd_, message));
    HandshakeResponse response;
    bool sock_closed;
    if (!io_utils::RecvMessage(gateway_sock_fd_, &response, &sock_closed)) {
        if (sock_closed) {
            LOG(FATAL) << "Gateway socket closed remotely";
        } else {
            PLOG(FATAL) << "Failed to read from gateway socket";
        }
    }
    if (Status{response.status} != Status::OK) {
        LOG(FATAL) << "Handshake failed";
    }
    client_id_ = response.client_id;
    LOG(INFO) << "Handshake done";
}

void FuncWorker::GatewayIpcThreadMain() {
    while (true) {
        Message message;
        bool sock_closed;
        if (!io_utils::RecvMessage(gateway_sock_fd_, &message, &sock_closed)) {
            if (sock_closed || errno == EBADF) {
                LOG(WARNING) << "Gateway socket closed remotely";
                gateway_disconnected_.store(true);
                return;
            } else {
                PLOG(FATAL) << "Failed to read from gateway socket";
            }
        }
#ifdef __FAAS_ENABLE_PROFILING
        gateway_message_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(
            GetMonotonicMicroTimestamp() - message.send_timestamp));
#endif
        MessageType type{message.message_type};
        if (type == MessageType::FUNC_CALL_COMPLETE || type == MessageType::FUNC_CALL_FAILED) {
            uint64_t call_id = message.func_call.full_call_id;
            {
                absl::MutexLock lk(&invoke_func_mu_);
                if (func_invoke_contexts_.contains(call_id)) {
                    FuncInvokeContext* context = func_invoke_contexts_[call_id].get();
                    if (type == MessageType::FUNC_CALL_COMPLETE) {
                        context->success = true;
                    } else {
                        context->success = false;
                    }
#ifdef __FAAS_ENABLE_PROFILING
                    context->processing_time = message.processing_time;
#endif
                    context->finished.Notify();
                } else {
                    LOG(ERROR) << "Cannot find InvokeContext for call_id " << call_id;
                }
            }
        } else {
            LOG(FATAL) << "Unknown message type";
        }
    }
}

bool FuncWorker::RunFuncHandler(void* worker_handle, uint64_t call_id) {
    utils::SharedMemory::Region* input_region = shared_memory_->OpenReadOnly(
        utils::SharedMemory::InputPath(call_id));
    input_size_stat_.AddSample(input_region->size());
    func_output_buffer_.Reset();
    int ret = func_call_fn_(
        worker_handle, input_region->base(), input_region->size());
    input_region->Close();
    {
        absl::MutexLock lk(&invoke_func_mu_);
        for (const auto& entry : func_invoke_contexts_) {
            FuncInvokeContext* context = entry.second.get();
            context->input_region->Close(true);
            if (context->output_region != nullptr) {
                context->output_region->Close(true);
            }
        }
        func_invoke_contexts_.clear();
    }
    if (ret != 0) {
        return false;
    }
    utils::SharedMemory::Region* output_region = shared_memory_->Create(
        utils::SharedMemory::OutputPath(call_id), func_output_buffer_.length());
    output_size_stat_.AddSample(func_output_buffer_.length());
    if (func_output_buffer_.length() > 0) {
        memcpy(output_region->base(), func_output_buffer_.data(),
               func_output_buffer_.length());
    }
    output_region->Close();
    return true;
}

bool FuncWorker::InvokeFunc(const char* func_name, const char* input_data, size_t input_length,
                            const char** output_data, size_t* output_length) {
    if (input_length == 0) {
        return false;
    }
    const FuncConfig::Entry* func_entry = func_config_.find_by_func_name(
        std::string_view(func_name, strlen(func_name)));
    if (func_entry == nullptr) {
        return false;
    }
    FuncInvokeContext* context = new FuncInvokeContext;
    context->success = false;
#ifdef __FAAS_ENABLE_PROFILING
    int64_t start_timestamp = GetMonotonicMicroTimestamp();
    context->processing_time = 0;
#endif
    context->output_region = nullptr;
    FuncCall func_call;
    func_call.func_id = gsl::narrow_cast<uint16_t>(func_entry->func_id);
    func_call.client_id = client_id_;
    func_call.call_id = next_call_id_.fetch_add(1);
    context->input_region = shared_memory_->Create(
        utils::SharedMemory::InputPath(func_call.full_call_id), input_length);
    memcpy(context->input_region->base(), input_data, input_length);
    Message message = {
#ifdef __FAAS_ENABLE_PROFILING
        .send_timestamp = GetMonotonicMicroTimestamp(),
        .processing_time = 0,
#endif
        .message_type = gsl::narrow_cast<uint16_t>(MessageType::INVOKE_FUNC),
        .func_call = func_call
    };
    {
        absl::MutexLock lk(&invoke_func_mu_);
        func_invoke_contexts_[func_call.full_call_id] = std::unique_ptr<FuncInvokeContext>(context);
        PCHECK(io_utils::SendMessage(gateway_sock_fd_, message));
    }
    context->finished.WaitForNotification();
    if (context->success) {
        context->output_region = shared_memory_->OpenReadOnly(
            utils::SharedMemory::OutputPath(func_call.full_call_id));
        *output_data = context->output_region->base();
        *output_length = context->output_region->size();
#ifdef __FAAS_ENABLE_PROFILING
        int32_t end2end_time = gsl::narrow_cast<int32_t>(
            GetMonotonicMicroTimestamp() - start_timestamp);
        system_protocol_overhead_stat_.AddSample(end2end_time - context->processing_time);
#endif
        return true;
    } else {
        return false;
    }
}

void FuncWorker::AppendOutputWrapper(void* caller_context, const char* data, size_t length) {
    FuncWorker* self = reinterpret_cast<FuncWorker*>(caller_context);
    self->func_output_buffer_.AppendData(data, length);
}

int FuncWorker::InvokeFuncWrapper(void* caller_context, const char* func_name,
                                  const char* input_data, size_t input_length,
                                  const char** output_data, size_t* output_length) {
    FuncWorker* self = reinterpret_cast<FuncWorker*>(caller_context);
    bool success = self->InvokeFunc(func_name, input_data, input_length, output_data, output_length);
    return success ? 0 : -1;
}

}  // namespace worker_v1
}  // namespace faas
