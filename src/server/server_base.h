#pragma once

#include "base/common.h"
#include "common/uv.h"
#include "utils/appendable_buffer.h"
#include "server/connection_base.h"
#include "server/io_worker.h"

namespace faas {
namespace server {

class ServerBase : public uv::Base {
public:
    static constexpr size_t kDefaultIOWorkerBufferSize = 65536;

    ServerBase();
    virtual ~ServerBase();

    void Start();
    void ScheduleStop();
    void WaitForFinish();

protected:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    uv_loop_t* uv_loop() { return &uv_loop_; }

    IOWorker* CreateIOWorker(std::string_view worker_name,
                             size_t read_buffer_size = kDefaultIOWorkerBufferSize,
                             size_t write_buffer_size = kDefaultIOWorkerBufferSize);
    void RegisterConnection(IOWorker* io_worker, ConnectionBase* connection,
                            uv_stream_t* uv_handle);

    // Supposed to be implemented by sub-class
    virtual void StartInternal() {}
    virtual void StopInternal() {}
    virtual void OnConnectionClose(ConnectionBase* connection) {}

private:
    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    absl::flat_hash_set<std::unique_ptr<IOWorker>> io_workers_;
    absl::flat_hash_map<IOWorker*, std::unique_ptr<uv_pipe_t>> pipes_to_io_worker_;
    utils::AppendableBuffer return_connection_read_buffer_;
    int next_connection_id_;

    void EventLoopThreadMain();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);
    DECLARE_UV_READ_CB_FOR_CLASS(ReturnConnection);
    DECLARE_UV_WRITE_CB_FOR_CLASS(PipeWrite2);

    DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

}  // namespace server
}  // namespace faas
