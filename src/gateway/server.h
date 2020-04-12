#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "common/uv.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "utils/buffer_pool.h"
#include "utils/shared_memory.h"
#include "gateway/connection.h"
#include "gateway/io_worker.h"
#include "gateway/http_request_context.h"
#include "gateway/http_connection.h"
#include "gateway/grpc_connection.h"
#include "gateway/message_connection.h"

namespace faas {
namespace gateway {

class Server : public uv::Base {
public:
    static constexpr int kDefaultListenBackLog = 32;
    static constexpr int kDefaultNumHttpWorkers = 1;
    static constexpr int kDefaultNumIpcWorkers = 1;
    static constexpr size_t kHttpConnectionBufferSize = 4096;
    static constexpr size_t kMessageConnectionBufferSize = 256;

    static constexpr int kMaxClientId = (1 << protocol::kClientIdBits) - 1;

    Server();
    ~Server();

    void set_address(std::string_view address) { address_ = std::string(address); }
    void set_port(int port) { port_ = port; }
    void set_grpc_port(int port) { grpc_port_ = port; }
    void set_ipc_path(std::string_view address) { ipc_path_ = std::string(address); }
    void set_listen_backlog(int value) { listen_backlog_ = value; }
    void set_num_http_workers(int value) { num_http_workers_ = value; }
    void set_num_ipc_workers(int value) { num_ipc_workers_ = value; }
    void set_num_io_workers(int value) { num_io_workers_ = value; }
    void set_shared_mem_path(std::string_view path) {
        shared_mem_path_ = std::string(path);
    }
    void set_func_config_file(std::string_view path) {
        func_config_file_ = std::string(path);
    }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    typedef std::function<bool(std::string_view /* method */,
                               std::string_view /* path */)> RequestMatcher;
    typedef std::function<void(HttpSyncRequestContext*)> SyncRequestHandler;
    typedef std::function<void(std::shared_ptr<HttpAsyncRequestContext>)> AsyncRequestHandler;

    // mathcer and handler must be thread-safe
    void RegisterSyncRequestHandler(RequestMatcher matcher, SyncRequestHandler handler);
    void RegisterAsyncRequestHandler(RequestMatcher matcher, AsyncRequestHandler handler);

    class RequestHandler {
    public:
        bool async() const { return async_; }

        void CallSync(HttpSyncRequestContext* context) const {
            DCHECK(!async_);
            sync_handler_(context);
        }

        void CallAsync(std::shared_ptr<HttpAsyncRequestContext> context) const {
            DCHECK(async_);
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

    bool MatchRequest(std::string_view method, std::string_view path,
                      const RequestHandler** request_handler) const;

    // Must be thread-safe
    void OnNewHandshake(MessageConnection* connection,
                        const protocol::HandshakeMessage& message,
                        protocol::HandshakeResponse* response);
    void OnRecvMessage(MessageConnection* connection, const protocol::Message& message);
    void OnNewGrpcCall(std::shared_ptr<GrpcCallContext> call_context);

private:
    class ExternalFuncCallContext;
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    std::string address_;
    int port_;
    int grpc_port_;
    std::string ipc_path_;
    int listen_backlog_;
    int num_http_workers_;
    int num_ipc_workers_;
    int num_io_workers_;
    std::string shared_mem_path_;
    std::string func_config_file_;

    uv_loop_t uv_loop_;
    uv_tcp_t uv_http_handle_;
    uv_tcp_t uv_grpc_handle_;
    uv_pipe_t uv_ipc_handle_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    std::vector<std::unique_ptr<IOWorker>> io_workers_;
    std::vector<IOWorker*> http_workers_;
    std::vector<IOWorker*> ipc_workers_;
    absl::flat_hash_map<IOWorker*, std::unique_ptr<uv_pipe_t>> pipes_to_io_worker_;
    utils::AppendableBuffer return_connection_read_buffer_;

    absl::flat_hash_set<std::unique_ptr<HttpConnection>> http_connections_;
    absl::flat_hash_set<std::unique_ptr<GrpcConnection>> grpc_connections_;

    int next_http_connection_id_;
    int next_grpc_connection_id_;
    int next_http_worker_id_;
    int next_ipc_worker_id_;

    std::vector<std::unique_ptr<RequestHandler>> request_handlers_;

    absl::Mutex message_connection_mu_;
    std::atomic<uint16_t> next_client_id_;
    absl::flat_hash_map<uint16_t, MessageConnection*>
        message_connections_by_client_id_ ABSL_GUARDED_BY(message_connection_mu_);
    absl::flat_hash_map<uint16_t, MessageConnection*>
        watchdog_connections_by_func_id_ ABSL_GUARDED_BY(message_connection_mu_);
    absl::flat_hash_set<std::unique_ptr<MessageConnection>> message_connections_;

    FuncConfig func_config_;
    std::atomic<uint32_t> next_call_id_;
    std::unique_ptr<utils::SharedMemory> shared_memory_;
    absl::Mutex external_func_calls_mu_;
    absl::flat_hash_map<uint64_t, std::unique_ptr<ExternalFuncCallContext>>
        external_func_calls_ ABSL_GUARDED_BY(external_func_calls_mu_);

    stat::StatisticsCollector<int32_t> message_delay_stat_;

    void InitAndStartIOWorker(IOWorker* io_worker);
    std::unique_ptr<uv_pipe_t> CreatePipeToWorker(int* pipe_fd_for_worker);
    void TransferConnectionToWorker(IOWorker* io_worker, Connection* connection,
                                    uv_stream_t* send_handle);
    void ReturnConnection(Connection* connection);

    void RegisterInternalRequestHandlers();
    protocol::FuncCall NewFuncCall(uint16_t func_id, uint16_t method_id = 0);
    void OnExternalFuncCall(uint16_t func_id,
                            std::shared_ptr<HttpAsyncRequestContext> http_context);
    void NewExternalFuncCall(std::unique_ptr<ExternalFuncCallContext> func_call_context);

    void EventLoopThreadMain();
    IOWorker* PickHttpWorker();
    IOWorker* PickIpcWorker();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);
    DECLARE_UV_CONNECTION_CB_FOR_CLASS(HttpConnection);
    DECLARE_UV_CONNECTION_CB_FOR_CLASS(GrpcConnection);
    DECLARE_UV_CONNECTION_CB_FOR_CLASS(MessageConnection);
    DECLARE_UV_READ_CB_FOR_CLASS(ReturnConnection);
    DECLARE_UV_WRITE_CB_FOR_CLASS(PipeWrite2);

    DISALLOW_COPY_AND_ASSIGN(Server);
};

}  // namespace gateway
}  // namespace faas
