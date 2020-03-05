#pragma once

#include "base/common.h"
#include "utils/buffer_pool.h"
#include "gateway/connection.h"
#include "gateway/io_worker.h"
#include "gateway/request_context.h"
#include "gateway/watchdog_pipe.h"

namespace faas {
namespace gateway {

class Server {
public:
    static constexpr int kDefaultListenBackLog = 32;
    static constexpr int kDefaultNumIOWorkers = 2;
    static constexpr size_t kWatchdogPipeBufferSize = 256;

    Server();
    ~Server();

    void set_address(absl::string_view address) { address_ = std::string(address); }
    void set_ipc_path(absl::string_view address) { ipc_path_ = std::string(address); }
    void set_port(int port) { port_ = port; }
    void set_listen_backlog(int value) { listen_backlog_ = value; }
    void set_num_io_workers(int value) { num_io_workers_ = value; }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    typedef std::function<bool(absl::string_view /* method */,
                               absl::string_view /* path */)> RequestMatcher;
    typedef std::function<void(SyncRequestContext*)> SyncRequestHandler;
    typedef std::function<void(std::shared_ptr<AsyncRequestContext>)> AsyncRequestHandler;

    // mathcer and handler must be thread-safe
    void RegisterSyncRequestHandler(RequestMatcher matcher, SyncRequestHandler handler);
    void RegisterAsyncRequestHandler(RequestMatcher matcher, AsyncRequestHandler handler);

    class RequestHandler {
    public:
        bool async() const { return async_; }

        void CallSync(SyncRequestContext* context) const {
            CHECK(!async_);
            sync_handler_(context);
        }

        void CallAsync(std::shared_ptr<AsyncRequestContext> context) const {
            CHECK(async_);
            async_handler_(std::move(context));
        }

    private:
        bool async_;
        RequestMatcher matcher_;
        SyncRequestHandler sync_handler_;
        AsyncRequestHandler async_handler_;

        friend class Server;

        RequestHandler(RequestMatcher matcher, SyncRequestHandler handler)
            : async_(false), matcher_(matcher), sync_handler_(handler) {}
        RequestHandler(RequestMatcher matcher, AsyncRequestHandler handler)
            : async_(true), matcher_(matcher), async_handler_(handler) {}

        DISALLOW_COPY_AND_ASSIGN(RequestHandler);
    };

    bool MatchRequest(absl::string_view method, absl::string_view path,
                      const RequestHandler** request_handler) const;
    
    void OnWatchdogPipeClose(WatchdogPipe* watchdog_pipe);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    std::string address_;
    std::string ipc_path_;
    int port_;
    int listen_backlog_;
    int num_io_workers_;

    uv_loop_t uv_loop_;
    uv_tcp_t uv_tcp_handle_;
    uv_pipe_t uv_ipc_handle_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    std::vector<std::unique_ptr<IOWorker>> io_workers_;
    absl::flat_hash_map<IOWorker*, std::unique_ptr<uv_pipe_t>> pipes_to_io_worker_;

    absl::flat_hash_set<Connection*> active_connections_;
    std::vector<Connection*> idle_connections_;
    std::vector<std::unique_ptr<Connection>> connections_;
    utils::AppendableBuffer return_connection_read_buffer_;

    int next_connection_id_;
    int next_io_worker_id_;

    std::vector<std::unique_ptr<RequestHandler>> request_handlers_;

    absl::flat_hash_map<WatchdogPipe*, std::unique_ptr<WatchdogPipe>> watchdog_pipes_;
    utils::BufferPool buffer_pool_for_watchdog_pipes_;

    std::unique_ptr<uv_pipe_t> CreatePipeToWorker(int* pipe_fd_for_worker);
    void TransferConnectionToWorker(IOWorker* io_worker, Connection* connection);
    void ReturnConnection(Connection* connection);

    void EventLoopThreadMain();
    IOWorker* PickIOWorker();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);
    DECLARE_UV_CONNECTION_CB_FOR_CLASS(Connection);
    DECLARE_UV_READ_CB_FOR_CLASS(ReturnConnection);
    DECLARE_UV_WRITE_CB_FOR_CLASS(PipeWrite2);
    DECLARE_UV_CONNECTION_CB_FOR_CLASS(WatchdogConnection);

    DISALLOW_COPY_AND_ASSIGN(Server);
};

}  // namespace gateway
}  // namespace faas
