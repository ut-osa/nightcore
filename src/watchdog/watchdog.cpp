#include "watchdog/watchdog.h"

#define HLOG(l) LOG(l) << "Watchdog: "
#define HVLOG(l) VLOG(l) << "Watchdog: "

namespace faas {
namespace watchdog {

using protocol::Status;
using protocol::Role;
using protocol::HandshakeMessage;
using protocol::HandshakeResponse;
using protocol::MessageType;
using protocol::FuncCall;
using protocol::Message;

Watchdog::Watchdog()
    : state_(kCreated), func_id_(-1), client_id_(0),
      event_loop_thread_("Watchdog_EventLoop",
                         std::bind(&Watchdog::EventLoopThreadMain, this)),
      gateway_connection_(this), next_func_worker_id_(0),
      buffer_pool_for_data_pipes_("DataPipe", kDataPipeBufferSize) {
    UV_CHECK_OK(uv_loop_init(&uv_loop_));
    uv_loop_.data = &event_loop_thread_;
    UV_CHECK_OK(uv_async_init(&uv_loop_, &stop_event_, &Watchdog::StopCallback));
    stop_event_.data = this;
}

Watchdog::~Watchdog() {
    State state = state_.load();
    CHECK(state == kCreated || state == kStopped);
    UV_CHECK_OK(uv_loop_close(&uv_loop_));
}

void Watchdog::ScheduleStop() {
    HLOG(INFO) << "Scheduled to stop";
    UV_CHECK_OK(uv_async_send(&stop_event_));
}

void Watchdog::Start() {
    CHECK(state_.load() == kCreated);
    CHECK(func_id_ != -1);
    CHECK(!fprocess_.empty());
    CHECK(run_mode_ == RunMode::SERIALIZING);
    // Create shared memory pool
    CHECK(!shared_mem_path_.empty());
    shared_memory_ = absl::make_unique<utils::SharedMemory>(shared_mem_path_);
    // Connect to gateway via IPC path
    uv_pipe_t* pipe_handle = gateway_connection_.uv_pipe_handle();
    UV_CHECK_OK(uv_pipe_init(&uv_loop_, pipe_handle, 0));
    HandshakeMessage message;
    message.role = static_cast<uint16_t>(Role::WATCHDOG);
    message.func_id = func_id_;
    gateway_connection_.Start(gateway_ipc_path_, message);
    // Start thread for running event loop
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void Watchdog::WaitForFinish() {
    CHECK(state_.load() != kCreated);
    event_loop_thread_.Join();
    CHECK(state_.load() == kStopped);
    HLOG(INFO) << "Stopped";
}

void Watchdog::OnGatewayConnectionClose() {
    HLOG(ERROR) << "Connection to gateway disconnected";
    ScheduleStop();
}

void Watchdog::EventLoopThreadMain() {
    HLOG(INFO) << "Event loop starts";
    int ret = uv_run(&uv_loop_, UV_RUN_DEFAULT);
    if (ret != 0) {
        HLOG(WARNING) << "uv_run returns non-zero value: " << ret;
    }
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

bool Watchdog::OnRecvHandshakeResponse(const HandshakeResponse& response) {
    CHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    if (static_cast<Status>(response.status) != Status::OK) {
        HLOG(WARNING) << "Handshake failed, will close the connection";
        gateway_connection_.ScheduleClose();
        return false;
    }
    client_id_ = response.client_id;
    return true;
}

void Watchdog::OnRecvMessage(const protocol::Message& message) {
    CHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    MessageType type = static_cast<MessageType>(message.message_type);
    if (type == MessageType::INVOKE_FUNC) {
        FuncCall func_call = message.func_call;
        if (func_call.func_id != func_id_) {
            HLOG(ERROR) << "I am not running func_id " << func_call.func_id;
            return;
        }
        auto func_worker = absl::make_unique<FuncWorker>(
            this, next_func_worker_id_++, run_mode_, fprocess_);
        func_worker->set_full_call_id(func_call.full_call_id);
        func_worker->Start(&uv_loop_, &buffer_pool_for_data_pipes_);
        utils::SharedMemory::Region* region = shared_memory_->OpenReadOnly(
            absl::StrCat(func_call.full_call_id, ".i"));
        FuncWorker* func_worker_ptr = func_worker.get();
        func_worker->WriteToStdin(
            region->base(), region->size(), [this, region, func_worker_ptr] (int status) {
                if (status != 0) {
                    HLOG(WARNING) << "Failed to write input data to FuncWorker";
                } else {
                    func_worker_ptr->StdinEof();
                }
                region->Close();
            });
        func_workers_.insert(std::move(func_worker));
    } else {
        HLOG(ERROR) << "Unknown message type!";
    }
}

void Watchdog::OnFuncWorkerExit(FuncWorker* func_worker) {
    CHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    CHECK(func_workers_.contains(func_worker));
    FuncCall func_call;
    func_call.full_call_id = func_worker->full_call_id();
    if (func_worker->exit_status() != 0) {
        HLOG(WARNING) << "FuncWorker[" << func_worker->id() << "] exits abnormally with code "
                      << func_worker->exit_status();
    }
    utils::AppendableBuffer* stdout_buffer = func_worker->StdoutBuffer();
    if (stdout_buffer->length() == 0) {
        HLOG(WARNING) << "FuncWorker[" << func_worker->id() << "] gives empty output";
    }
    if (func_worker->exit_status() != 0 || stdout_buffer->length() == 0) {
        gateway_connection_.WriteWMessage({
            .message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED),
            .func_call = func_call
        });
    } else {
        utils::SharedMemory::Region* region = shared_memory_->Create(
            absl::StrCat(func_call.full_call_id, ".o"), stdout_buffer->length());
        memcpy(region->base(), stdout_buffer->data(), stdout_buffer->length());
        region->Close();
        gateway_connection_.WriteWMessage({
            .message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE),
            .func_call = func_call
        });
    }
    func_workers_.erase(func_worker);
}

UV_ASYNC_CB_FOR_CLASS(Watchdog, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    gateway_connection_.ScheduleClose();
    uv_close(reinterpret_cast<uv_handle_t*>(&stop_event_), nullptr);
    state_.store(kStopping);
}

}  // namespace watchdog
}  // namespace faas
