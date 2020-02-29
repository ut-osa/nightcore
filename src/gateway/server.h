#pragma once

#include "base/common.h"
#include "gateway/connection.h"
#include "gateway/io_worker.h"

namespace faas {
namespace gateway {

class Server {
public:
    static constexpr const char* kDefaultListenAddress = "127.0.0.1";
    static constexpr int kDefaultListenPort = 8080;
    static constexpr int kDefaultListenBackLog = 32;
    static constexpr int kDefaultNumIOWorkers = 2;

    Server();
    ~Server();

    void set_address(const std::string& address) { address_ = address; }
    void set_port(int port) { port_ = port; }
    void set_listen_backlog(int value) { listen_backlog_ = value; }
    void set_num_io_workers(int value) { num_io_workers_ = value; }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

private:
    enum State { kReady, kRunning, kStopping, kStopped };
    State state_;

    std::string address_;
    int port_;
    int listen_backlog_;
    int num_io_workers_;

    uv_loop_t uv_loop_;
    uv_tcp_t uv_tcp_handle_;
    uv_async_t stop_event_;

    std::unique_ptr<std::thread> thread_;
    std::vector<std::unique_ptr<IOWorker>> io_workers_;
    absl::flat_hash_map<IOWorker*, std::unique_ptr<uv_pipe_t>> pipes_to_io_worker_;

    absl::flat_hash_set<Connection*> active_connections_;
    std::vector<Connection*> idle_connections_;
    std::vector<std::unique_ptr<Connection>> connections_;
    utils::AppendableBuffer return_connection_read_buffer_;

    int next_connection_id_;
    int next_io_worker_id_;

    std::unique_ptr<uv_pipe_t> CreatePipeToWorker(int* pipe_fd_for_worker);
    void TransferConnectionToWorker(IOWorker* io_worker, Connection* connection);
    void ReturnConnection(Connection* connection);

    void EventLoopThreadMain();
    IOWorker* PickIOWorker();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);
    DECLARE_UV_CONNECTION_CB_FOR_CLASS(Connection);
    DECLARE_UV_READ_CB_FOR_CLASS(ReturnConnection);
    DECLARE_UV_WRITE_CB_FOR_CLASS(PipeWrite2);

    DISALLOW_COPY_AND_ASSIGN(Server);
};

}  // namespace gateway
}  // namespace faas
