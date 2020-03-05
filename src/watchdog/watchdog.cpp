#include "watchdog/watchdog.h"

#define HLOG(l) LOG(l) << "Watchdog: "
#define HVLOG(l) VLOG(l) << "Watchdog: "

namespace faas {
namespace watchdog {

Watchdog::Watchdog()
    : state_(kCreated),
      event_loop_thread_("Watchdog_EventLoop",
                         std::bind(&Watchdog::EventLoopThreadMain, this)),
      gateway_pipe_(this), buffer_pool_for_pipes_("Pipe", kPipeBufferSize) {
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
    CHECK(!function_name_.empty());
    CHECK(!fprocess_.empty());
    // Connect to gateway via IPC path
    uv_pipe_t* pipe_handle = gateway_pipe_.uv_pipe_handle();
    UV_CHECK_OK(uv_pipe_init(&uv_loop_, pipe_handle, 0));
    gateway_pipe_.Start(gateway_ipc_path_, function_name_, &buffer_pool_for_pipes_);
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

void Watchdog::OnGatewayPipeClose() {
    HLOG(WARNING) << "Pipe to gateway disconnected";
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

UV_ASYNC_CB_FOR_CLASS(Watchdog, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    gateway_pipe_.ScheduleClose();
    uv_close(reinterpret_cast<uv_handle_t*>(&stop_event_), nullptr);
    state_.store(kStopping);
}

}  // namespace watchdog
}  // namespace faas
