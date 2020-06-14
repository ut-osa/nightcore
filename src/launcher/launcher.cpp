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

constexpr size_t Launcher::kSubprocessPipeBufferSize;

Launcher::Launcher()
    : state_(kCreated), func_id_(-1), fprocess_multi_worker_mode_(false),
      event_loop_thread_("Launcher/EL",
                         absl::bind_front(&Launcher::EventLoopThreadMain, this)),
      gateway_connection_(this),
      gateway_message_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("gateway_message_delay")) {
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
    buffer_pool_for_subprocess_pipes_ = std::make_unique<utils::BufferPool>(
        "SubprocessPipe", kSubprocessPipeBufferSize);
    // Connect to gateway via IPC path
    uv_pipe_t* pipe_handle = gateway_connection_.uv_pipe_handle();
    UV_DCHECK_OK(uv_pipe_init(&uv_loop_, pipe_handle, 0));
    Message handshake_message = NewLauncherHandshakeMessage(func_id_);
    std::string self_container_id = docker_utils::GetSelfContainerId();
    DCHECK_EQ(self_container_id.size(), docker_utils::kContainerIdLength);
    SetInlineDataInMessage(&handshake_message, std::span<const char>(self_container_id.data(),
                                                                     self_container_id.size()));
    gateway_connection_.Start(ipc::GetGatewayUnixSocketPath(), std::move(handshake_message));
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

void Launcher::OnGatewayConnectionClose() {
    HLOG(ERROR) << "Connection to gateway disconnected";
    ScheduleStop();
}

void Launcher::OnFuncProcessExit(FuncProcess* func_process) {
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
        gateway_connection_.ScheduleClose();
        return false;
    }
    if (!func_config_.Load(std::string_view(payload.data(), payload.size()))) {
        HLOG(ERROR) << "Failed to load function config from handshake response, will close the connection";
        gateway_connection_.ScheduleClose();
        return false;
    }
    return true;
}

void Launcher::OnRecvMessage(const protocol::Message& message) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    gateway_message_delay_stat_.AddSample(ComputeMessageDelay(message));
    if (IsCreateFuncWorkerMessage(message)) {
        uint16_t client_id = message.client_id;
        auto func_process = std::make_unique<FuncProcess>(
            this, func_processes_.size(), client_id);
        if (func_process->Start(&uv_loop_, buffer_pool_for_subprocess_pipes_.get())) {
            func_processes_.push_back(std::move(func_process));
        } else {
            HLOG(FATAL) << "Failed to start function process!";
        }
    } else {
        HLOG(ERROR) << "Unknown message type!";
    }
}

UV_ASYNC_CB_FOR_CLASS(Launcher, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    gateway_connection_.ScheduleClose();
    uv_close(UV_AS_HANDLE(&stop_event_), nullptr);
    state_.store(kStopping);
}

}  // namespace launcher
}  // namespace faas
