#pragma once

#include "base/common.h"
#include "utils/uv_utils.h"
#include "gateway/connection.h"

namespace faas {
namespace gateway {

class Server;

class IOWorker {
public:
    static constexpr size_t kReadBufferSize = 4096;

    IOWorker(Server* server, int worker_id);
    ~IOWorker();

    int id() { return worker_id_; }

    void Start(int pipe_to_server_fd);
    void ScheduleStop();
    void WaitForFinish();

    // Called by Connection for ONLY once
    void OnConnectionClose(Connection* connection);

    // Can only be called from uv_loop_
    void NewReadBuffer(size_t suggested_size, uv_buf_t* buf);
    void ReturnReadBuffer(const uv_buf_t* buf);

private:
    enum State { kReady, kRunning, kStopping, kStopped };

    Server* server_;
    int worker_id_;
    State state_;

    std::string log_header_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    uv_pipe_t pipe_to_server_;

    std::unique_ptr<std::thread> thread_;
    absl::flat_hash_set<Connection*> connections_;

    std::vector<char*> available_read_buffers_;
    std::vector<std::unique_ptr<char[]>> all_read_buffers_;

    void EventLoopThreadMain();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);
    DECLARE_UV_READ_CB_FOR_CLASS(NewConnection);
    DECLARE_UV_WRITE_CB_FOR_CLASS(PipeWrite);

    DISALLOW_COPY_AND_ASSIGN(IOWorker);
};

}  // namespace gateway
}  // namespace faas
