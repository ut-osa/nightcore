#pragma once

#include "base/common.h"
#include "utils/uv_utils.h"
#include "utils/appendable_buffer.h"
#include "utils/buffer_pool.h"
#include "watchdog/run_mode.h"

namespace faas {
namespace watchdog {

class Watchdog;

class FuncWorker {
public:
    FuncWorker(Watchdog* watchdog, int worker_id,
               RunMode run_mode, absl::string_view fprocess);
    ~FuncWorker();

    int id() const { return worker_id_; }
    int exit_status() const { return exit_status_; }

    void set_full_call_id(uint64_t value) { full_call_id_ = value; }
    uint64_t full_call_id() const { return full_call_id_; }

    void Start(uv_loop_t* uv_loop, utils::BufferPool* buffer_pool);
    void ScheduleExit();

    void WriteToStdin(const char* data, size_t length,
                      std::function<void(int)> callback);
    void StdinEof();
    utils::AppendableBuffer* StdoutBuffer() { return &stdout_; }
    utils::AppendableBuffer* StderrBuffer() { return &stderr_; }

private:
    enum State { kCreated, kRunning, kExited };

    Watchdog* watchdog_;
    int worker_id_;
    State state_;
    RunMode run_mode_;
    std::string fprocess_;
    uint64_t full_call_id_;
    int exit_status_;
    int closed_uv_handles_;
    int total_uv_handles_;

    std::string log_header_;

    uv_process_t uv_process_handle_;
    uv_pipe_t uv_stdin_pipe_;
    uv_pipe_t uv_stdout_pipe_;
    uv_pipe_t uv_stderr_pipe_;
    bool stdin_eof_;

    utils::BufferPool* buffer_pool_;
    utils::AppendableBuffer stdout_;
    utils::AppendableBuffer stderr_;

    void BuildReadablePipe(uv_loop_t* uv_loop, uv_pipe_t* uv_pipe,
                           uv_stdio_container_t* stdio_container);
    void BuildWritablePipe(uv_loop_t* uv_loop, uv_pipe_t* uv_pipe,
                           uv_stdio_container_t* stdio_container);

    void CopyRecvData(ssize_t nread, const uv_buf_t* inbuf,
                      utils::AppendableBuffer* outbuf);

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadStdout);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadStderr);
    DECLARE_UV_WRITE_CB_FOR_CLASS(WriteStdin);
    DECLARE_UV_EXIT_CB_FOR_CLASS(ProcessExit);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace watchdog
}  // namespace faas
