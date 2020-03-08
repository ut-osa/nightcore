#pragma once

#include "base/common.h"
#include "utils/uv_utils.h"
#include "utils/appendable_buffer.h"
#include "utils/buffer_pool.h"

namespace faas {
namespace watchdog {

class Subprocess {
public:
    static constexpr size_t kDefaultMaxStdoutSize = 16 * 1024 * 1024;  // 16MB‬
    static constexpr size_t kDefaultMaxStderrSize = 1 * 1024 * 1024;   // 1MB
    static constexpr const char* kShellPath = "/bin/bash";

    explicit Subprocess(absl::string_view cmd,
                        size_t max_stdout_size = kDefaultMaxStdoutSize,
                        size_t max_stderr_size = kDefaultMaxStderrSize);
    ~Subprocess();

    typedef std::function<void(int /* exit_status */, absl::Span<const char> /* stdout */,
                               absl::Span<const char> /* stderr */)> ExitCallback;

    bool Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool,
               ExitCallback exit_callback);
    void Kill(int signum = SIGKILL);

    // Caller should not close stdin pipe by itself, but to call CloseStdin()
    uv_pipe_t* StdinPipe();
    void CloseStdin();

    void WriteToStdin(const char* data, size_t length,
                      std::function<void(int)> callback);
    void StdinEof();

private:
    enum State { kCreated, kRunning, kExited, kClosed };

    State state_;
    std::string cmd_;
    size_t max_stdout_size_;
    size_t max_stderr_size_;
    int exit_status_;
    ExitCallback exit_callback_;
    int closed_uv_handles_;
    int total_uv_handles_;

    uv_process_t uv_process_handle_;
    uv_pipe_t uv_stdin_pipe_;
    uv_pipe_t uv_stdout_pipe_;
    uv_pipe_t uv_stderr_pipe_;
    bool stdin_pipe_closed_;

    utils::BufferPool* read_buffer_pool_;
    utils::AppendableBuffer stdout_;
    utils::AppendableBuffer stderr_;

    void BuildReadablePipe(uv_loop_t* uv_loop, uv_pipe_t* uv_pipe,
                           uv_stdio_container_t* stdio_container);
    void BuildWritablePipe(uv_loop_t* uv_loop, uv_pipe_t* uv_pipe,
                           uv_stdio_container_t* stdio_container);

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadStdout);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadStderr);
    DECLARE_UV_EXIT_CB_FOR_CLASS(ProcessExit);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(Subprocess);
};

}  // namespace watchdog
}  // namespace faas
