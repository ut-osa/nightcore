#include "worker/v1/func_worker.h"

#include "base/protocol.h"
#include "utils/io.h"
#include "utils/socket.h"

namespace faas {
namespace worker_v1 {

using protocol::MessageType;
using protocol::Message;
using protocol::Role;
using protocol::Status;
using protocol::HandshakeMessage;
using protocol::HandshakeResponse;

FuncWorker::FuncWorker()
    : func_id_(-1), input_pipe_fd_(-1), output_pipe_fd_(-1),
      gateway_sock_fd_(-1), gateway_disconnected_(false),
      gateway_ipc_thread_("GatewayIpc", std::bind(&FuncWorker::GatewayIpcThreadMain, this)) {}

FuncWorker::~FuncWorker() {
    close(gateway_sock_fd_);
    gateway_ipc_thread_.Join();
}

void FuncWorker::Serve() {
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
    shared_memory_ = absl::make_unique<utils::SharedMemory>(shared_mem_path_);
    // Connect to gateway via IPC path
    CHECK(!gateway_ipc_path_.empty());
    CHECK(func_id_ != -1);
    gateway_sock_fd_ = utils::UnixDomainSocketConnect(gateway_ipc_path_);
    GatewayIpcHandshake();
    gateway_ipc_thread_.Start();
    // Enter main serving loop
    MainServingLoop();
}

void FuncWorker::MainServingLoop() {
    void* func_worker;
    CHECK(create_func_worker_fn_(&func_worker) == 0)
        << "Failed to create function worker";

    while (true) {
        Message message;
        bool input_pipe_closed;
        if (!utils::RecvMessage(input_pipe_fd_, &message, &input_pipe_closed)) {
            if (input_pipe_closed) {
                LOG(WARNING) << "Pipe to watchdog closed remotely";
                break;
            } else {
                PLOG(FATAL) << "Failed to read from watchdog pipe";
            }
        }
        MessageType type = static_cast<MessageType>(message.message_type);
        if (type == MessageType::INVOKE_FUNC) {
             Message response;
             response.func_call = message.func_call;
             bool success = InvokeFunc(func_worker, message.func_call.full_call_id);
             if (success) {
                 response.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
             } else {
                 response.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
             }
             if (success) {
                 PCHECK(utils::SendMessage(gateway_sock_fd_, response));
             }
             PCHECK(utils::SendMessage(output_pipe_fd_, response));
        } else {
            LOG(FATAL) << "Unknown message type";
        }
    }

    CHECK(destroy_func_worker_fn_(func_worker) == 0)
        << "Failed to destroy function worker";
}

void FuncWorker::GatewayIpcHandshake() {
    HandshakeMessage message = {
        .role = static_cast<uint16_t>(Role::FUNC_WORKER),
        .func_id = static_cast<uint16_t>(func_id_)
    };
    PCHECK(utils::SendMessage(gateway_sock_fd_, message));
    HandshakeResponse response;
    bool sock_closed;
    if (!utils::RecvMessage(gateway_sock_fd_, &response, &sock_closed)) {
        if (sock_closed) {
            LOG(FATAL) << "Gateway socket closed remotely";
        } else {
            PLOG(FATAL) << "Failed to read from gateway socket";
        }
    }
    if (static_cast<Status>(response.status) != Status::OK) {
        LOG(FATAL) << "Handshake failed";
    }
    client_id_ = response.client_id;
    LOG(INFO) << "Handshake done";
}

void FuncWorker::GatewayIpcThreadMain() {
    while (true) {
        Message message;
        bool sock_closed;
        if (!utils::RecvMessage(gateway_sock_fd_, &message, &sock_closed)) {
            if (sock_closed || errno == EBADF) {
                LOG(WARNING) << "Gateway socket closed remotely";
                gateway_disconnected_.store(true);
                return;
            } else {
                PLOG(FATAL) << "Failed to read from gateway socket";
            }
        }
    }
}

bool FuncWorker::InvokeFunc(void* worker_handle, uint64_t call_id) {
    utils::SharedMemory::Region* input_region = shared_memory_->OpenReadOnly(
        absl::StrCat(call_id, ".i"));
    func_output_buffer_.Reset();
    int ret = func_call_fn_(
        worker_handle, input_region->base(), input_region->size(),
        this, nullptr, &FuncWorker::AppendOutputWrapper);
    input_region->Close();
    if (ret != 0) {
        return false;
    }
    utils::SharedMemory::Region* output_region = shared_memory_->Create(
        absl::StrCat(call_id, ".o"), func_output_buffer_.length());
    memcpy(output_region->base(), func_output_buffer_.data(),
           func_output_buffer_.length());
    output_region->Close();
    return true;
}

void FuncWorker::AppendOutputWrapper(void* caller_context, const char* data, size_t length) {
    FuncWorker* self = reinterpret_cast<FuncWorker*>(caller_context);
    self->func_output_buffer_.AppendData(data, length);
}

}  // namespace worker_v1
}  // namespace faas
