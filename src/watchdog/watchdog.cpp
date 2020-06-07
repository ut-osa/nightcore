#include "watchdog/watchdog.h"

#include "ipc/base.h"
#include "common/time.h"
#include "utils/fs.h"

#define HLOG(l) LOG(l) << "Watchdog: "
#define HVLOG(l) VLOG(l) << "Watchdog: "

namespace faas {
namespace watchdog {

using protocol::FuncCall;
using protocol::Message;
using protocol::IsHandshakeResponseMessage;
using protocol::IsInvokeFuncMessage;
using protocol::NewWatchdogHandshakeMessage;
using protocol::NewFuncCallCompleteMessage;
using protocol::NewFuncCallFailedMessage;
using protocol::GetFuncCallFromMessage;

constexpr size_t Watchdog::kSubprocessPipeBufferSizeForSerializingMode;
constexpr size_t Watchdog::kSubprocessPipeBufferSizeForFuncWorkerMode;
constexpr absl::Duration Watchdog::kMinFuncWorkerCreationInterval;

Watchdog::Watchdog()
    : state_(kCreated), func_id_(-1), client_id_(0),
      event_loop_thread_("Watchdog/EL",
                         absl::bind_front(&Watchdog::EventLoopThreadMain, this)),
      gateway_connection_(this), next_func_worker_id_(0),
      gateway_message_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("gateway_message_delay")),
      func_worker_message_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("func_worker_message_delay")),
      processing_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("processing_delay")),
      func_worker_decision_counter_(
          stat::CategoryCounter::StandardReportCallback("func_worker_decision")),
      func_worker_load_counter_(
          stat::CategoryCounter::StandardReportCallback("func_worker_load")),
      incoming_requests_counter_(
          stat::Counter::StandardReportCallback("incoming_requests")) {
    UV_DCHECK_OK(uv_loop_init(&uv_loop_));
    uv_loop_.data = &event_loop_thread_;
    UV_DCHECK_OK(uv_async_init(&uv_loop_, &stop_event_, &Watchdog::StopCallback));
    stop_event_.data = this;
}

Watchdog::~Watchdog() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
    UV_DCHECK_OK(uv_loop_close(&uv_loop_));
    DCHECK(func_runners_.empty());
}

void Watchdog::ScheduleStop() {
    HLOG(INFO) << "Scheduled to stop";
    UV_DCHECK_OK(uv_async_send(&stop_event_));
}

void Watchdog::Start() {
    DCHECK(state_.load() == kCreated);
    CHECK(func_id_ != -1);
    CHECK(!fprocess_.empty());
    switch (run_mode_) {
    case RunMode::SERIALIZING:
        buffer_pool_for_subprocess_pipes_ = std::make_unique<utils::BufferPool>(
            "SubprocessPipe", kSubprocessPipeBufferSizeForSerializingMode);
        break;
    case RunMode::FUNC_WORKER_FIXED:
    case RunMode::FUNC_WORKER_ON_DEMAND:
    case RunMode::FUNC_WORKER_ASYNC: {
        if (!func_worker_output_dir_.empty() && !fs_utils::Exists(func_worker_output_dir_)) {
            CHECK(fs_utils::MakeDirectory(func_worker_output_dir_));
        }
        buffer_pool_for_subprocess_pipes_ = std::make_unique<utils::BufferPool>(
            "SubprocessPipe", kSubprocessPipeBufferSizeForFuncWorkerMode);
        int initial_num_func_workers_ = max_num_func_workers_;
        if (run_mode_ == RunMode::FUNC_WORKER_ON_DEMAND) {
            initial_num_func_workers_ = min_num_func_workers_;
        }
        for (int i = 0; i < initial_num_func_workers_; i++) {
            auto func_worker = std::make_unique<FuncWorker>(this, i, run_mode_ == RunMode::FUNC_WORKER_ASYNC);
            func_worker->Start(&uv_loop_, buffer_pool_for_subprocess_pipes_.get());
            func_workers_.push_back(std::move(func_worker));
        }
        last_func_worker_creation_time_ = absl::Now();
        HLOG(INFO) << "Initial number of FuncWorker: " << initial_num_func_workers_;
        break;
    }
    default:
        HLOG(FATAL) << "Unknown run mode";
    }
    // Connect to gateway via IPC path
    uv_pipe_t* pipe_handle = gateway_connection_.uv_pipe_handle();
    UV_DCHECK_OK(uv_pipe_init(&uv_loop_, pipe_handle, 0));
    gateway_connection_.Start(ipc::GetGatewayUnixSocketPath(),
                              NewWatchdogHandshakeMessage(func_id_));
    // Start thread for running event loop
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void Watchdog::WaitForFinish() {
    DCHECK(state_.load() != kCreated);
    event_loop_thread_.Join();
    DCHECK(state_.load() == kStopped);
    HLOG(INFO) << "Stopped";
}

void Watchdog::OnGatewayConnectionClose() {
    HLOG(ERROR) << "Connection to gateway disconnected";
    ScheduleStop();
}

void Watchdog::EventLoopThreadMain() {
    base::Thread::current()->MarkThreadCategory("IO");
    HLOG(INFO) << "Event loop starts";
    int ret = uv_run(&uv_loop_, UV_RUN_DEFAULT);
    if (ret != 0) {
        HLOG(WARNING) << "uv_run returns non-zero value: " << ret;
    }
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

bool Watchdog::OnRecvHandshakeResponse(const Message& handshake_response,
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

void Watchdog::OnRecvMessage(const protocol::Message& message) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
#ifdef __FAAS_ENABLE_PROFILING
    gateway_message_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(
        GetMonotonicMicroTimestamp() - message.send_timestamp));
#endif
    if (IsInvokeFuncMessage(message)) {
        FuncCall func_call = GetFuncCallFromMessage(message);
        if (func_call.func_id != func_id_) {
            HLOG(ERROR) << "I am not running func_id " << func_call.func_id;
            return;
        }
        incoming_requests_counter_.Tick();
        FuncRunner* func_runner;
        switch (run_mode_) {
        case RunMode::SERIALIZING:
            func_runner = new SerializingFuncRunner(
                this, func_call.full_call_id,
                buffer_pool_for_subprocess_pipes_.get());
            break;
        case RunMode::FUNC_WORKER_FIXED:
        case RunMode::FUNC_WORKER_ON_DEMAND:
        case RunMode::FUNC_WORKER_ASYNC:
            func_runner = new WorkerFuncRunner(
                this, func_call.full_call_id, PickFuncWorker());
            break;
        default:
            LOG(FATAL) << "Unknown run mode";
        }
        func_runners_[func_call.full_call_id] = std::unique_ptr<FuncRunner>(func_runner);
        func_runner->Start(&uv_loop_);
    } else {
        HLOG(ERROR) << "Unknown message type!";
    }
}

void Watchdog::OnFuncRunnerComplete(FuncRunner* func_runner, FuncRunner::Status status,
                                    int32_t processing_time) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    DCHECK(func_runners_.contains(func_runner->call_id()));
    FuncCall func_call;
    func_call.full_call_id = func_runner->call_id();
    func_runners_.erase(func_runner->call_id());
    if (state_.load() == kStopping) {
        HLOG(WARNING) << "Watchdog is scheduled to close, will not write to gateway connection";
        return;
    }
    if (status == FuncRunner::kSuccess) {
        if (run_mode_ == RunMode::SERIALIZING) {
            Message message = NewFuncCallCompleteMessage(func_call);
#ifdef __FAAS_ENABLE_PROFILING
            message.send_timestamp = GetMonotonicMicroTimestamp();
            message.processing_time = processing_time;
#endif
            gateway_connection_.WriteMessage(message);
        }
        processing_delay_stat_.AddSample(processing_time);
    } else {
        Message message = NewFuncCallFailedMessage(func_call);
#ifdef __FAAS_ENABLE_PROFILING
        message.send_timestamp = GetMonotonicMicroTimestamp();
        message.processing_time = processing_time;
#endif
        gateway_connection_.WriteMessage(message);
    }
}

void Watchdog::OnFuncWorkerClose(FuncWorker* func_worker) {
    HLOG(WARNING) << "FuncWorker[" << func_worker->id() << "] closed";
}

void Watchdog::OnFuncWorkerIdle(FuncWorker* func_worker) {
    DCHECK(run_mode_ != RunMode::FUNC_WORKER_ASYNC);
    if (run_mode_ == RunMode::FUNC_WORKER_ON_DEMAND) {
        idle_func_workers_.push_back(func_worker);
    }
}

FuncWorker* Watchdog::PickFuncWorker() {
    FuncWorker* func_worker;
    if (run_mode_ == RunMode::FUNC_WORKER_FIXED || run_mode_ == RunMode::FUNC_WORKER_ASYNC) {
        func_worker = func_workers_[next_func_worker_id_].get();
        next_func_worker_id_ = (next_func_worker_id_ + 1) % func_workers_.size();
    } else if (run_mode_ == RunMode::FUNC_WORKER_ON_DEMAND) {
        if (idle_func_workers_.empty()
                && gsl::narrow_cast<int>(func_workers_.size()) < max_num_func_workers_
                && absl::Now() >= last_func_worker_creation_time_ + kMinFuncWorkerCreationInterval) {
            auto new_func_worker = std::make_unique<FuncWorker>(this, func_workers_.size());
            new_func_worker->Start(&uv_loop_, buffer_pool_for_subprocess_pipes_.get());
            func_workers_.push_back(std::move(new_func_worker));
            HLOG(INFO) << "Create new FuncWorker, current count is " << func_workers_.size();
            DCHECK(!idle_func_workers_.empty());
            last_func_worker_creation_time_ = absl::Now();
        }
        if (!idle_func_workers_.empty()) {
            func_worker = idle_func_workers_.back();
            idle_func_workers_.pop_back();
            DCHECK(func_worker->is_idle());
            func_worker_decision_counter_.Tick(0);
        } else {
            int idx = absl::Uniform<int>(random_bit_gen_, 0, func_workers_.size());
            func_worker = func_workers_[idx].get();
            func_worker_decision_counter_.Tick(1);
        }
    } else {
        HLOG(FATAL) << "Should not reach here!";
    }
    func_worker_load_counter_.Tick(func_worker->id());
    return func_worker;
}

UV_ASYNC_CB_FOR_CLASS(Watchdog, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    gateway_connection_.ScheduleClose();
    for (const auto& entry : func_runners_) {
        FuncRunner* func_runner = entry.second.get();
        func_runner->ScheduleStop();
    }
    for (const auto& func_worker : func_workers_) {
        func_worker->ScheduleClose();
    }
    uv_close(UV_AS_HANDLE(&stop_event_), nullptr);
    state_.store(kStopping);
}

}  // namespace watchdog
}  // namespace faas
