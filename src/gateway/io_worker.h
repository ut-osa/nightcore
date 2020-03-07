#pragma once

#include "base/common.h"
#include "utils/uv_utils.h"
#include "utils/buffer_pool.h"
#include "gateway/connection.h"

namespace faas {
namespace gateway {

class Server;

class IOWorker {
public:
    static constexpr size_t kDefaultBufferSize = 4096;

    IOWorker(Server* server, absl::string_view worker_name,
             size_t read_buffer_size = kDefaultBufferSize,
             size_t write_buffer_size = kDefaultBufferSize);
    ~IOWorker();

    absl::string_view worker_name() const { return worker_name_; }

    void Start(int pipe_to_server_fd);
    void ScheduleStop();
    void WaitForFinish();

    // Called by Connection for ONLY once
    void OnConnectionClose(Connection* connection);

    // Can only be called from uv_loop_
    void NewReadBuffer(size_t suggested_size, uv_buf_t* buf);
    void ReturnReadBuffer(const uv_buf_t* buf);
    void NewWriteBuffer(uv_buf_t* buf);
    void ReturnWriteBuffer(char* buf);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };

    ABSL_ATTRIBUTE_UNUSED Server* server_;
    std::string worker_name_;
    std::atomic<State> state_;

    std::string log_header_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    uv_pipe_t pipe_to_server_;

    base::Thread event_loop_thread_;
    absl::flat_hash_set<Connection*> connections_;
    utils::BufferPool read_buffer_pool_;
    utils::BufferPool write_buffer_pool_;

    void EventLoopThreadMain();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);
    DECLARE_UV_READ_CB_FOR_CLASS(NewConnection);
    DECLARE_UV_WRITE_CB_FOR_CLASS(PipeWrite);

    DISALLOW_COPY_AND_ASSIGN(IOWorker);
};

}  // namespace gateway
}  // namespace faas
