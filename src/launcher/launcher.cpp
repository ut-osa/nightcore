#include "launcher/launcher.h"

#include "ipc/base.h"
#include "common/time.h"
#include "utils/fs.h"
#include "utils/docker.h"

#define HLOG(l) LOG(l) << "Launcher: "
#define HVLOG(l) VLOG(l) << "Launcher: "

namespace faas {
namespace launcher {

using protocol::FuncCall;
using protocol::Message;
using protocol::IsHandshakeResponseMessage;
using protocol::IsCreateFuncWorkerMessage;
using protocol::NewLauncherHandshakeMessage;
using protocol::SetInlineDataInMessage;
using protocol::ComputeMessageDelay;

Launcher::Launcher()
    : state_(kCreated), func_id_(-1), fprocess_mode_(kInvalidMode), engine_tcp_port_(-1),
      event_loop_thread_("Launcher/EL",
                         absl::bind_front(&Launcher::EventLoopThreadMain, this)),
      buffer_pool_("Launcher", kBufferSize),
      func_worker_use_engine_socket_(false),
      engine_connection_(this),
      engine_message_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("engine_message_delay")) {
    UV_DCHECK_OK(uv_loop_init(&uv_loop_));
    uv_loop_.data = &event_loop_thread_;
    UV_DCHECK_OK(uv_async_init(&uv_loop_, &stop_event_, &Launcher::StopCallback));
    stop_event_.data = this;
}

Launcher::~Launcher() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
    UV_DCHECK_OK(uv_loop_close(&uv_loop_));
}

void Launcher::ScheduleStop() {
    HLOG(INFO) << "Scheduled to stop";
    UV_DCHECK_OK(uv_async_send(&stop_event_));
}

void Launcher::Start() {
    DCHECK(state_.load() == kCreated);
    CHECK(func_id_ != -1);
    CHECK(!fprocess_.empty());
    // Connect to engine via IPC path
    Message handshake_message = NewLauncherHandshakeMessage(func_id_);
    std::string self_container_id = docker_utils::GetSelfContainerId();
    DCHECK_EQ(self_container_id.size(), docker_utils::kContainerIdLength);
    SetInlineDataInMessage(&handshake_message, std::span<const char>(self_container_id.data(),
                                                                     self_container_id.size()));
    engine_connection_.Start(&uv_loop_, engine_tcp_port_, handshake_message);
    // Start thread for running event loop
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void Launcher::WaitForFinish() {
    DCHECK(state_.load() != kCreated);
    event_loop_thread_.Join();
    DCHECK(state_.load() == kStopped);
    HLOG(INFO) << "Stopped";
}

void Launcher::OnEngineConnectionClose() {
    HLOG(ERROR) << "Connection to engine disconnected";
    ScheduleStop();
}

void Launcher::OnFuncProcessExit(FuncProcess* func_process) {
    if (fprocess_mode_ == kGoMode) {
        HLOG(FATAL) << "Golang fprocess exited";
    }
    if (fprocess_mode_ == kNodeJsMode) {
        HLOG(FATAL) << "Node.js fprocess exited";
    }
    if (fprocess_mode_ == kPythonMode) {
        HLOG(FATAL) << "Python fprocess exited";
    }
    int id = func_process->id();
    HLOG(WARNING) << "Function process " << id << " terminated";
    DCHECK_GE(id, 0);
    DCHECK_LT(id, gsl::narrow_cast<int>(func_processes_.size()));
    DCHECK(func_processes_[id].get() == func_process);
    func_processes_[id].reset(nullptr);
}

void Launcher::EventLoopThreadMain() {
    base::Thread::current()->MarkThreadCategory("IO");
    HLOG(INFO) << "Event loop starts";
    int ret = uv_run(&uv_loop_, UV_RUN_DEFAULT);
    if (ret != 0) {
        HLOG(WARNING) << "uv_run returns non-zero value: " << ret;
    }
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

bool Launcher::OnRecvHandshakeResponse(const Message& handshake_response,
                                       std::span<const char> payload) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    if (!IsHandshakeResponseMessage(handshake_response)) {
        HLOG(ERROR) << "Invalid handshake response, will close the connection";
        engine_connection_.ScheduleClose();
        return false;
    }
    if (handshake_response.flags & protocol::kFuncWorkerUseEngineSocketFlag) {
        func_worker_use_engine_socket_ = true;
    }
    if (!func_config_.Load(std::string_view(payload.data(), payload.size()))) {
        HLOG(ERROR) << "Failed to load function config from handshake response, will close the connection";
        engine_connection_.ScheduleClose();
        return false;
    }
    func_config_json_.assign(payload.data(), payload.size());
    return true;
}

void Launcher::OnRecvMessage(const protocol::Message& message) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    engine_message_delay_stat_.AddSample(ComputeMessageDelay(message));
    if (IsCreateFuncWorkerMessage(message)) {
        if (fprocess_mode_ == kCppMode) {
            auto func_process = std::make_unique<FuncProcess>(
                this, /* id= */ func_processes_.size(),
                /* initial_client_id= */ message.client_id);
            if (func_process->Start(&uv_loop_, &buffer_pool_)) {
                func_processes_.push_back(std::move(func_process));
            } else {
                HLOG(FATAL) << "Failed to start function process!";
            }
        } else if (fprocess_mode_ == kGoMode
                   || fprocess_mode_ == kNodeJsMode
                   || fprocess_mode_ == kPythonMode) {
            if (func_processes_.empty()) {
                auto func_process = std::make_unique<FuncProcess>(
                    this, /* id= */ 0, /* initial_client_id= */ message.client_id);
                if (func_process->Start(&uv_loop_, &buffer_pool_)) {
                    func_processes_.push_back(std::move(func_process));
                } else {
                    HLOG(FATAL) << "Failed to start function process!";
                }
            } else {
                FuncProcess* func_process = func_processes_[0].get();
                func_process->SendMessage(message);
            }
        } else {
            HLOG(FATAL) << "Unreachable";
        }
    } else {
        HLOG(ERROR) << "Unknown message type!";
    }
}

void Launcher::NewReadBuffer(size_t suggested_size, uv_buf_t* buf) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    buffer_pool_.Get(buf);
}

void Launcher::ReturnReadBuffer(const uv_buf_t* buf) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    buffer_pool_.Return(buf);
}

void Launcher::NewWriteBuffer(uv_buf_t* buf) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    buffer_pool_.Get(buf);
}

void Launcher::ReturnWriteBuffer(char* buf) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    buffer_pool_.Return(buf);
}

uv_write_t* Launcher::NewWriteRequest() {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    return write_req_pool_.Get();
}

void Launcher::ReturnWriteRequest(uv_write_t* write_req) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    write_req_pool_.Return(write_req);
}

UV_ASYNC_CB_FOR_CLASS(Launcher, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    engine_connection_.ScheduleClose();
    for (auto& func_process : func_processes_) {
        func_process->ScheduleClose();
    }
    uv_close(UV_AS_HANDLE(&stop_event_), nullptr);
    state_.store(kStopping);
}

}  // namespace launcher
}  // namespace faas
