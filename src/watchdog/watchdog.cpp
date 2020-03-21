#include "watchdog/watchdog.h"

#include "common/time.h"
#include "utils/fs.h"

#include <absl/random/distributions.h>

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

constexpr size_t Watchdog::kSubprocessPipeBufferSizeForSerializingMode;
constexpr size_t Watchdog::kSubprocessPipeBufferSizeForFuncWorkerMode;

Watchdog::Watchdog()
    : state_(kCreated), func_id_(-1), client_id_(0),
      event_loop_thread_("Watchdog_EventLoop",
                         std::bind(&Watchdog::EventLoopThreadMain, this)),
      gateway_connection_(this), next_func_worker_id_(0),
      gateway_message_delay_stat_(
          stat::StatisticsCollector<uint32_t>::StandardReportCallback("gateway_message_delay")),
      func_worker_message_delay_stat_(
          stat::StatisticsCollector<uint32_t>::StandardReportCallback("func_worker_message_delay")) {
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
    // Load function config file
    CHECK(!func_config_file_.empty());
    CHECK(func_config_.Load(func_config_file_));
    CHECK(func_id_ != -1);
    CHECK(func_config_.find_by_func_id(func_id_) != nullptr);
    CHECK(!fprocess_.empty());
    if (run_mode_ == RunMode::SERIALIZING) {
        buffer_pool_for_subprocess_pipes_ = absl::make_unique<utils::BufferPool>(
            "SubprocessPipe", kSubprocessPipeBufferSizeForSerializingMode);
    } else if (run_mode_ == RunMode::FUNC_WORKER_FIXED || run_mode_ == RunMode::FUNC_WORKER_ON_DEMAND) {
        if (!func_worker_output_dir_.empty() && !fs_utils::Exists(func_worker_output_dir_)) {
            CHECK(fs_utils::MakeDirectory(func_worker_output_dir_));
        }
        buffer_pool_for_subprocess_pipes_ = absl::make_unique<utils::BufferPool>(
            "SubprocessPipe", kSubprocessPipeBufferSizeForFuncWorkerMode);
        int initial_num_func_workers_ = run_mode_ == RunMode::FUNC_WORKER_FIXED
            ? max_num_func_workers_
            : min_num_func_workers_;
        for (int i = 0; i < initial_num_func_workers_; i++) {
            auto func_worker = absl::make_unique<FuncWorker>(this, i);
            func_worker->Start(&uv_loop_, buffer_pool_for_subprocess_pipes_.get());
            func_workers_.push_back(std::move(func_worker));
        }
        HLOG(INFO) << "Initial number of FuncWorker: " << initial_num_func_workers_;
    } else {
        HLOG(FATAL) << "Unknown run mode";
    }
    // Create shared memory pool
    CHECK(!shared_mem_path_.empty());
    shared_memory_ = absl::make_unique<utils::SharedMemory>(shared_mem_path_);
    // Connect to gateway via IPC path
    uv_pipe_t* pipe_handle = gateway_connection_.uv_pipe_handle();
    UV_DCHECK_OK(uv_pipe_init(&uv_loop_, pipe_handle, 0));
    HandshakeMessage message;
    message.role = static_cast<uint16_t>(Role::WATCHDOG);
    message.func_id = func_id_;
    gateway_connection_.Start(gateway_ipc_path_, message);
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
    HLOG(INFO) << "Event loop starts";
    int ret = uv_run(&uv_loop_, UV_RUN_DEFAULT);
    if (ret != 0) {
        HLOG(WARNING) << "uv_run returns non-zero value: " << ret;
    }
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

bool Watchdog::OnRecvHandshakeResponse(const HandshakeResponse& response) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    if (static_cast<Status>(response.status) != Status::OK) {
        HLOG(WARNING) << "Handshake failed, will close the connection";
        gateway_connection_.ScheduleClose();
        return false;
    }
    client_id_ = response.client_id;
    return true;
}

void Watchdog::OnRecvMessage(const protocol::Message& message) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    MessageType type = static_cast<MessageType>(message.message_type);
    gateway_message_delay_stat_.AddSample(
        GetMonotonicMicroTimestamp() - message.send_timestamp);
    if (type == MessageType::INVOKE_FUNC) {
        FuncCall func_call = message.func_call;
        if (func_call.func_id != func_id_) {
            HLOG(ERROR) << "I am not running func_id " << func_call.func_id;
            return;
        }
        FuncRunner* func_runner;
        switch (run_mode_) {
        case RunMode::SERIALIZING:
            func_runner = new SerializingFuncRunner(
                this, func_call.full_call_id,
                buffer_pool_for_subprocess_pipes_.get(), shared_memory_.get());
            break;
        case RunMode::FUNC_WORKER_FIXED:
        case RunMode::FUNC_WORKER_ON_DEMAND:
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

void Watchdog::OnFuncRunnerComplete(FuncRunner* func_runner, FuncRunner::Status status, uint32_t processing_time) {
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
            gateway_connection_.WriteMessage({
                .message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE),
                .func_call = func_call,
                .send_timestamp = GetMonotonicMicroTimestamp(),
                .processing_time = processing_time
            });
        }
    } else {
        gateway_connection_.WriteMessage({
            .message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED),
            .func_call = func_call,
            .send_timestamp = GetMonotonicMicroTimestamp(),
            .processing_time = processing_time
        });
    }
}

void Watchdog::OnFuncWorkerClose(FuncWorker* func_worker) {
    HLOG(WARNING) << "FuncWorker[" << func_worker->id() << "] closed";
}

void Watchdog::OnFuncWorkerIdle(FuncWorker* func_worker) {
    idle_func_workers_.push_back(func_worker);
}

FuncWorker* Watchdog::PickFuncWorker() {
    FuncWorker* func_worker;
    if (run_mode_ == RunMode::FUNC_WORKER_FIXED) {
        func_worker = func_workers_[next_func_worker_id_].get();
        next_func_worker_id_ = (next_func_worker_id_ + 1) % func_workers_.size();
    } else if (run_mode_ == RunMode::FUNC_WORKER_ON_DEMAND) {
        if (idle_func_workers_.empty()
                && static_cast<int>(func_workers_.size()) < max_num_func_workers_) {
            auto new_func_worker = absl::make_unique<FuncWorker>(this, func_workers_.size());
            new_func_worker->Start(&uv_loop_, buffer_pool_for_subprocess_pipes_.get());
            func_workers_.push_back(std::move(new_func_worker));
            HLOG(INFO) << "Create new FuncWorker, current count is " << func_workers_.size();
            DCHECK(!idle_func_workers_.empty());
        }
        if (!idle_func_workers_.empty()) {
            func_worker = idle_func_workers_.back();
            idle_func_workers_.pop_back();
            DCHECK(func_worker->is_idle());
        } else {
            int idx = absl::Uniform<int>(random_bit_gen_, 0, func_workers_.size());
            func_worker = func_workers_[idx].get();
        }
    } else {
        HLOG(FATAL) << "Should not reach here!";
    }
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
