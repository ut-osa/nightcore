#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "utils/uv_utils.h"
#include "utils/buffer_pool.h"
#include "utils/object_pool.h"
#include "watchdog/subprocess.h"

namespace faas {
namespace watchdog {

class Watchdog;
class WorkerFuncRunner;

class FuncWorker : public uv::Base {
public:
    static constexpr size_t kWriteBufferSize = 64;
    static_assert(sizeof(protocol::Message) <= kWriteBufferSize,
                  "kWriteBufferSize is too small");

    FuncWorker(Watchdog* watchdog, int worker_id, bool async = false);
    ~FuncWorker();

    int id() const { return worker_id_; }

    void Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool);
    void ScheduleClose();

    bool ScheduleFuncCall(WorkerFuncRunner* func_runner, uint64_t call_id);
    bool is_idle() const { return state_ == kIdle; }

private:
    enum State { kCreated, kAsync, kIdle, kSending, kReceiving, kClosing, kClosed };

    State state_;
    Watchdog* watchdog_;
    int worker_id_;
    bool async_;

    std::string log_header_;

    uv_loop_t* uv_loop_;
    Subprocess subprocess_;
    int input_pipe_fd_;
    int output_pipe_fd_;
    uv_pipe_t* uv_input_pipe_handle_;
    uv_pipe_t* uv_output_pipe_handle_;
    utils::BufferPool* read_buffer_pool_;

    utils::AppendableBuffer recv_buffer_;
    absl::flat_hash_map<uint64_t, WorkerFuncRunner*> func_runners_;

    // Used in sync mode
    protocol::Message message_to_send_;
    uv_write_t write_req_;
    std::queue<uint64_t> pending_func_calls_;

    // Used in async mode
    std::unique_ptr<utils::BufferPool> write_buffer_pool_;
    std::unique_ptr<utils::SimpleObjectPool<uv_write_t>> write_req_pool_;
    int inflight_requests_;

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
