#pragma once

#ifndef __FAAS_SRC
#error common/subprocess.h cannot be included outside
#endif

#include "base/common.h"
#include "common/uv.h"
#include "utils/appendable_buffer.h"
#include "utils/buffer_pool.h"

namespace faas {
namespace uv {

class Subprocess : public Base {
public:
    static constexpr size_t kDefaultMaxStdoutSize = 16 * 1024 * 1024;  // 16MB‬
    static constexpr size_t kDefaultMaxStderrSize = 1 * 1024 * 1024;   // 1MB
    static constexpr const char* kShellPath = "/bin/bash";

    enum StandardPipe { kStdin = 0, kStdout = 1, kStderr = 2, kNumStdPipes = 3 };

    explicit Subprocess(std::string_view cmd,
                        size_t max_stdout_size = kDefaultMaxStdoutSize,
                        size_t max_stderr_size = kDefaultMaxStderrSize);
    ~Subprocess();

    int pid() const { return pid_; }

    // Open file for standard file
    void SetStandardFile(StandardPipe pipe, std::string_view file_path);

    // For both CreateReadPipe and CreateWritePipe, fd is returned. stdin, stdout,
    // and stderr pipes will be created automatically, thus new pipes start with fd 3. 
    // Also note that notions of readable/writable is from the perspective of created
    // subprocess.
    int CreateReadablePipe();
    int CreateWritablePipe();

    void SetWorkingDir(std::string_view path);
    void AddEnvVariable(std::string_view name, std::string_view value);
    void AddEnvVariable(std::string_view name, int value);

    typedef std::function<void(long /* exit_status */, std::span<const char> /* stdout */,
                               std::span<const char> /* stderr */)> ExitCallback;

    bool Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool,
               ExitCallback exit_callback);
    void Kill(int signum = SIGKILL);

    // Caller should not close pipe by itself, but to call ClosePipe with fd.
    // Note that the caller should NOT touch stdout (fd = 1) and stderr (fd = 2)
    // with GetPipe and ClosePipe. These two pipes are fully managed by Subprocess
    // class.
    uv_pipe_t* GetPipe(int fd);
    void ClosePipe(int fd);
    bool PipeClosed(int fd);

private:
    enum State { kCreated, kRunning, kExited, kClosed };

    State state_;
    std::string cmd_;
    size_t max_stdout_size_;
    size_t max_stderr_size_;
    long exit_status_;
    HandleScope handle_scope_;
    ExitCallback exit_callback_;

    std::vector<int> std_fds_;
    std::vector<uv_stdio_flags> pipe_types_;
    std::string working_dir_;
    std::vector<std::string> env_variables_;

    uv_process_t uv_process_handle_;
    std::vector<uv_pipe_t> uv_pipe_handles_;
    std::vector<bool> pipe_closed_;

    utils::BufferPool* read_buffer_pool_;
    utils::AppendableBuffer stdout_;
    utils::AppendableBuffer stderr_;

    int pid_;

    void OnAllHandlesClosed();

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadStdout);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadStderr);
    DECLARE_UV_EXIT_CB_FOR_CLASS(ProcessExit);

    DISALLOW_COPY_AND_ASSIGN(Subprocess);
};

}  // namespace uv
}  // namespace faas
