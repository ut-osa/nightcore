#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/uv.h"
#include "common/subprocess.h"
#include "utils/buffer_pool.h"
#include "utils/object_pool.h"

namespace faas {
namespace launcher {

class Launcher;

class FuncProcess : public uv::Base {
public:
    FuncProcess(Launcher* launcher, int id, int initial_client_id = -1);
    ~FuncProcess();

    int id() const { return id_; }

    bool Start(uv_loop_t* uv_loop, utils::BufferPool* read_buffer_pool);
    void SendMessage(const protocol::Message& message);
    void ScheduleClose();

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    State state_;
    Launcher* launcher_;
    int id_;
    int initial_client_id_;
    uint32_t initial_payload_size_;

    std::string log_header_;

    uv_loop_t* uv_loop_;
    uv::Subprocess subprocess_;
    utils::BufferPool* read_buffer_pool_;
    int message_pipe_fd_;
    uv_pipe_t* message_pipe_;

    void OnSubprocessExit(int exit_status, std::span<const char> stdout,
                          std::span<const char> stderr);

    DECLARE_UV_WRITE_CB_FOR_CLASS(SendMessage);

    DISALLOW_COPY_AND_ASSIGN(FuncProcess);
};

}  // namespace launcher
}  // namespace faas
