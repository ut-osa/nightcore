#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "common/uv.h"
#include "common/stat.h"
#include "utils/buffer_pool.h"
#include "utils/object_pool.h"
#include "server/connection_base.h"

namespace faas {
namespace server {

class IOWorker final : public uv::Base {
public:
    IOWorker(std::string_view worker_name, size_t read_buffer_size, size_t write_buffer_size);
    ~IOWorker();

    std::string_view worker_name() const { return worker_name_; }

    // Return current IOWorker within event loop thread
    static IOWorker* current() { return current_; }

    void Start(int pipe_to_server_fd);
    void ScheduleStop();
    void WaitForFinish();

    // Called by Connection for ONLY once
    void OnConnectionClose(ConnectionBase* connection);

    // Can only be called from uv_loop_
    void NewReadBuffer(size_t suggested_size, uv_buf_t* buf);
    void ReturnReadBuffer(const uv_buf_t* buf);
    void NewWriteBuffer(uv_buf_t* buf);
    void ReturnWriteBuffer(char* buf);
    uv_write_t* NewWriteRequest();
    void ReturnWriteRequest(uv_write_t* write_req);
    // Pick a connection of given type managed by this IOWorker
    ConnectionBase* PickConnection(int type);

    // Schedule a function to run on this IO worker's event loop
    // thread. It can be called safely from other threads.
    // When the function is ready to run, IO worker will check if its
    // owner connection is still active, and will not run the function
    // if it is closed.
    void ScheduleFunction(ConnectionBase* owner, std::function<void()> fn);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };

    std::string worker_name_;
    std::atomic<State> state_;
    static thread_local IOWorker* current_;

    std::string log_header_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    uv_pipe_t pipe_to_server_;
    uv_async_t run_fn_event_;

    base::Thread event_loop_thread_;
    absl::flat_hash_map</* id */ int, ConnectionBase*> connections_;
    absl::flat_hash_map</* type */ int, absl::flat_hash_set</* id */ int>> connections_by_type_;
    absl::flat_hash_map</* type */ int, std::vector<ConnectionBase*>> connections_for_pick_;
    absl::flat_hash_map</* type */ int, size_t> connections_for_pick_rr_;
    utils::BufferPool read_buffer_pool_;
    utils::BufferPool write_buffer_pool_;
    utils::SimpleObjectPool<uv_write_t> write_req_pool_;
    int connections_on_closing_;

    struct ScheduledFunction {
        int owner_id;
        std::function<void()> fn;
    };
    absl::Mutex scheduled_function_mu_;
    absl::InlinedVector<std::unique_ptr<ScheduledFunction>, 16>
        scheduled_functions_ ABSL_GUARDED_BY(scheduled_function_mu_);
    std::atomic<int64_t> async_event_recv_timestamp_;

    stat::StatisticsCollector<int32_t> uv_async_delay_stat_;

    void EventLoopThreadMain();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);
    DECLARE_UV_READ_CB_FOR_CLASS(NewConnection);
    DECLARE_UV_WRITE_CB_FOR_CLASS(PipeWrite);
    DECLARE_UV_ASYNC_CB_FOR_CLASS(RunScheduledFunctions);

    DISALLOW_COPY_AND_ASSIGN(IOWorker);
};

}  // namespace server
}  // namespace faas
