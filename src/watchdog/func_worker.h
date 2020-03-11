#pragma once

#include "base/common.h"
#include "base/protocol.h"
#include "utils/uv_utils.h"
#include "watchdog/subprocess.h"

namespace faas {
namespace watchdog {

class Watchdog;
class WorkerFuncRunner;

class FuncWorker {
public:
    FuncWorker(Watchdog* watchdog, absl::string_view fprocess, int worker_id);
    ~FuncWorker();

    int id() const { return worker_id_; }

    void Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool);
    void ScheduleClose();

    bool ScheduleFuncCall(WorkerFuncRunner* func_runner, uint64_t call_id);

private:
    enum State { kCreated, kIdle, kSending, kReceiving, kClosing, kClosed };

    State state_;
    Watchdog* watchdog_;
    int worker_id_;

    std::string log_header_;

    uv_loop_t* uv_loop_;
    Subprocess subprocess_;
    int input_pipe_fd_;
    int output_pipe_fd_;
    uv_pipe_t* uv_input_pipe_handle_;
    uv_pipe_t* uv_output_pipe_handle_;
    utils::BufferPool* read_buffer_pool_;

    utils::AppendableBuffer recv_buffer_;
    protocol::Message message_to_send_;
    uv_write_t write_req_;
    std::queue<uint64_t> pending_func_calls_;
    absl::flat_hash_map<uint64_t, WorkerFuncRunner*> func_runners_;

    void OnSubprocessExit(int exit_status, absl::Span<const char> stdout,
                          absl::Span<const char> stderr);
    void DispatchFuncCall(uint64_t call_id);
    void OnRecvMessage(const protocol::Message& message);

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadMessage);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteMessage);

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace watchdog
}  // namespace faas
