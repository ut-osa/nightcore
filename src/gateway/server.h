#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "common/uv.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "server/server_base.h"
#include "gateway/http_request_context.h"
#include "gateway/http_connection.h"
#include "gateway/grpc_connection.h"
#include "gateway/message_connection.h"
#include "gateway/dispatcher.h"
#include "gateway/worker_manager.h"
#include "gateway/monitor.h"
#include "gateway/tracer.h"

namespace faas {
namespace gateway {

class Server final : public server::ServerBase {
public:
    static constexpr int kDefaultListenBackLog = 32;
    static constexpr int kDefaultNumHttpWorkers = 1;
    static constexpr int kDefaultNumIpcWorkers = 1;
    static constexpr size_t kHttpConnectionBufferSize = 65536;
    static constexpr size_t kMessageConnectionBufferSize = __FAAS_MESSAGE_SIZE * 2;

    Server();
    ~Server();

    void set_address(std::string_view address) { address_ = std::string(address); }
    void set_http_port(int port) { http_port_ = port; }
    void set_grpc_port(int port) { grpc_port_ = port; }
    void set_listen_backlog(int value) { listen_backlog_ = value; }
    void set_num_http_workers(int value) { num_http_workers_ = value; }
    void set_num_ipc_workers(int value) { num_ipc_workers_ = value; }
    void set_num_io_workers(int value) { num_io_workers_ = value; }
    void set_func_config_file(std::string_view path) {
        func_config_file_ = std::string(path);
    }
    FuncConfig* func_config() { return &func_config_; }
    WorkerManager* worker_manager() { return worker_manager_.get(); }
    Monitor* monitor() { return monitor_.get(); }
    Tracer* tracer() { return tracer_.get(); }

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
    bool OnNewHandshake(MessageConnection* connection,
                        const protocol::Message& handshake_message,
                        protocol::Message* response,
                        std::span<const char>* response_payload);
    void OnRecvMessage(MessageConnection* connection, const protocol::Message& message);
    void OnNewGrpcCall(std::shared_ptr<GrpcCallContext> call_context);
    Dispatcher* GetOrCreateDispatcher(uint16_t func_id);
    void DiscardFuncCall(const protocol::FuncCall& func_call);

private:
    class ExternalFuncCallContext;

    std::string address_;
    int http_port_;
    int grpc_port_;
    int listen_backlog_;
    int num_http_workers_;
    int num_ipc_workers_;
    int num_io_workers_;
    std::string func_config_file_;

    uv_tcp_t uv_http_handle_;
    uv_tcp_t uv_grpc_handle_;
    uv_pipe_t uv_ipc_handle_;

    std::vector<server::IOWorker*> http_workers_;
    std::vector<server::IOWorker*> ipc_workers_;

    absl::flat_hash_set<HttpConnection*> http_connections_;
    absl::flat_hash_set<GrpcConnection*> grpc_connections_;

    int next_http_connection_id_;
    int next_grpc_connection_id_;
    int next_http_worker_id_;
    int next_ipc_worker_id_;

    std::vector<std::unique_ptr<RequestHandler>> request_handlers_;

    absl::flat_hash_set<MessageConnection*> message_connections_;
    std::unique_ptr<WorkerManager> worker_manager_;
    std::unique_ptr<Monitor> monitor_;
    std::unique_ptr<Tracer> tracer_;
    size_t max_running_external_requests_;

    absl::Mutex mu_;

    std::string func_config_json_;
    FuncConfig func_config_;
    std::atomic<uint32_t> next_call_id_;
    absl::flat_hash_map</* full_call_id */ uint64_t, std::unique_ptr<ExternalFuncCallContext>>
        running_external_func_calls_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* func_id */ uint16_t, std::unique_ptr<Dispatcher>>
        dispatchers_ ABSL_GUARDED_BY(mu_);
    std::queue<std::unique_ptr<ExternalFuncCallContext>>
        pending_external_func_calls_ ABSL_GUARDED_BY(mu_);
    std::vector<protocol::FuncCall> discarded_func_calls_ ABSL_GUARDED_BY(mu_);

    std::atomic<int> inflight_external_requests_;

    int64_t last_external_request_timestamp_ ABSL_GUARDED_BY(mu_);
    stat::Counter incoming_external_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<float> external_requests_instant_rps_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> inflight_external_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> pending_external_requests_stat_ ABSL_GUARDED_BY(mu_);

    stat::StatisticsCollector<int32_t> message_delay_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter input_use_shm_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter output_use_shm_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter discarded_func_call_stat_ ABSL_GUARDED_BY(mu_);

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;

    void RegisterInternalRequestHandlers();
    void OnExternalFuncCall(uint16_t func_id,
                            std::shared_ptr<HttpAsyncRequestContext> http_context);
    void NewExternalFuncCall(std::unique_ptr<ExternalFuncCallContext> func_call_context);

    server::IOWorker* PickHttpWorker();
    server::IOWorker* PickIpcWorker();

    Dispatcher* GetOrCreateDispatcherLocked(uint16_t func_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    bool DispatchExternalFuncCall(ExternalFuncCallContext* func_call_context);
    void ProcessDiscardedFuncCallIfNecessary();

    DECLARE_UV_CONNECTION_CB_FOR_CLASS(HttpConnection);
    DECLARE_UV_CONNECTION_CB_FOR_CLASS(GrpcConnection);
    DECLARE_UV_CONNECTION_CB_FOR_CLASS(MessageConnection);

    DISALLOW_COPY_AND_ASSIGN(Server);
};

}  // namespace gateway
}  // namespace faas
