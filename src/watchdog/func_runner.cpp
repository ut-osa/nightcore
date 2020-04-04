#include "watchdog/func_runner.h"

#include "common/time.h"
#include "watchdog/watchdog.h"
#include "watchdog/func_worker.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace watchdog {

void FuncRunner::Complete(Status status, uint32_t processing_time) {
    DCHECK(state_ != kCompleted);
    state_ = kCompleted;
    watchdog_->OnFuncRunnerComplete(this, status, processing_time);
}

SerializingFuncRunner::SerializingFuncRunner(Watchdog* watchdog, uint64_t call_id,
                                             utils::BufferPool* read_buffer_pool,
                                             utils::SharedMemory* shared_memory)
    : FuncRunner(watchdog, call_id), subprocess_(watchdog->fprocess()),
      read_buffer_pool_(read_buffer_pool), shared_memory_(shared_memory),
      input_region_(nullptr) {}

SerializingFuncRunner::~SerializingFuncRunner() {
    DCHECK(input_region_ == nullptr);
}

void SerializingFuncRunner::Start(uv_loop_t* uv_loop) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop);
    start_timestamp_ = GetMonotonicMicroTimestamp();
    if (!subprocess_.Start(uv_loop, read_buffer_pool_,
                           absl::bind_front(&SerializingFuncRunner::OnSubprocessExit, this))) {
        HLOG(ERROR) << "Failed to start fprocess";
        Complete(kFailedToStartProcess);
        return;
    }
    uv_pipe_t* subprocess_stdin = subprocess_.GetPipe(Subprocess::kStdin);
    input_region_ = shared_memory_->OpenReadOnly(utils::SharedMemory::InputPath(call_id_));
    uv_buf_t buf = {
        .base = const_cast<char*>(input_region_->base()),
        .len = input_region_->size()
    };
    subprocess_stdin->data = this;
    UV_DCHECK_OK(uv_write(&write_req_, UV_AS_STREAM(subprocess_stdin),
                          &buf, 1, &SerializingFuncRunner::WriteSubprocessStdinCallback));
    state_ = kRunning;
}

void SerializingFuncRunner::OnSubprocessExit(int exit_status,
                                             std::span<const char> stdout,
                                             std::span<const char> stderr) {
    if (exit_status != 0) {
        HLOG(WARNING) << "Subprocess exits with code " << exit_status;
        HVLOG(1) << "Subprocess's stderr: " << std::string_view(stderr.data(), stderr.size());
        Complete(kProcessExitAbnormally);
        return;
    }
    utils::SharedMemory::Region* region = shared_memory_->Create(
        utils::SharedMemory::OutputPath(call_id_), stdout.size());
    if (stdout.size() > 0) {
        memcpy(region->base(), stdout.data(), stdout.size());
    }
    region->Close();
#ifdef __FAAS_ENABLE_PROFILING
    Complete(kSuccess, GetMonotonicMicroTimestamp() - start_timestamp_);
#else
    Complete(kSuccess);
#endif
}

void SerializingFuncRunner::ScheduleStop() {
    subprocess_.Kill();
}

UV_WRITE_CB_FOR_CLASS(SerializingFuncRunner, WriteSubprocessStdin) {
    DCHECK(input_region_ != nullptr);
    input_region_->Close();
    input_region_ = nullptr;
    if (status == 0) {
        subprocess_.ClosePipe(Subprocess::kStdin);
    } else {
        HLOG(ERROR) << "Failed to write input data: " << uv_strerror(status);
        subprocess_.Kill();
    }
}

WorkerFuncRunner::WorkerFuncRunner(Watchdog* watchdog, uint64_t call_id,
                                   FuncWorker* func_worker)
    : FuncRunner(watchdog, call_id), func_worker_(func_worker) {}

WorkerFuncRunner::~WorkerFuncRunner() {}

void WorkerFuncRunner::Start(uv_loop_t* uv_loop) {
    if (func_worker_ == nullptr || !func_worker_->ScheduleFuncCall(this, call_id_)) {
        Complete(kFailedToSchedule);
    }
}

void WorkerFuncRunner::ScheduleStop() {
    // Barely do nothing
}

}  // namespace watchdog
}  // namespace faas
