#pragma once

#include "base/common.h"
#include "utils/uv_utils.h"
#include "utils/shared_memory.h"
#include "watchdog/subprocess.h"

namespace faas {
namespace watchdog {

class Watchdog;

class FuncRunner {
public:
    FuncRunner(Watchdog* watchdog, uint64_t call_id)
        : state_(kCreated), watchdog_(watchdog), call_id_(call_id),
          log_header_(absl::StrFormat("FuncRunner[%d]: ", call_id)) {}
    virtual ~FuncRunner() { CHECK(state_ != kRunning); }

    uint64_t call_id() const { return call_id_; }
    virtual void Start(uv_loop_t* uv_loop) = 0;

    enum Status {
        kSuccess,
        kFailedToStartProcess,
        kProcessExitAbnormally,
        kEmptyOutput
    };
    void Complete(Status status);

protected:
    enum State { kCreated, kRunning, kCompleted };
    State state_;
    Watchdog* watchdog_;
    uint64_t call_id_;
    std::string log_header_;

private:
    DISALLOW_COPY_AND_ASSIGN(FuncRunner);
};

class SerializingFuncRunner final : public FuncRunner {
public:
    SerializingFuncRunner(Watchdog* watchdog, uint64_t call_id,
                          utils::BufferPool* read_buffer_pool,
                          utils::SharedMemory* shared_memory);
    ~SerializingFuncRunner();

    void Start(uv_loop_t* uv_loop) override;

private:
    Subprocess subprocess_;
    utils::BufferPool* read_buffer_pool_;
    utils::SharedMemory* shared_memory_;

    uv_write_t write_req_;
    utils::SharedMemory::Region* input_region_;

    void OnSubprocessExit(int exit_status, absl::Span<const char> stdout,
                          absl::Span<const char> stderr);
    
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteSubprocessStdin);

    DISALLOW_COPY_AND_ASSIGN(SerializingFuncRunner);
};

}  // namespace watchdog
}  // namespace faas
