#include "gateway/server.h"

#include "ipc/base.h"
#include "ipc/shm_region.h"
#include "common/time.h"
#include "utils/fs.h"
#include "utils/io.h"
#include "utils/docker.h"
#include "worker/worker_lib.h"

#define HLOG(l) LOG(l) << "Server: "
#define HVLOG(l) VLOG(l) << "Server: "

namespace faas {
namespace gateway {

using protocol::FuncCall;
using protocol::FuncCallDebugString;
using protocol::NewFuncCall;
using protocol::NewFuncCallWithMethod;
using protocol::GatewayMessage;
using protocol::GetFuncCallFromMessage;
using protocol::IsEngineHandshakeMessage;
using protocol::IsFuncCallCompleteMessage;
using protocol::IsFuncCallFailedMessage;
using protocol::IsFuncCallDiscardedMessage;
using protocol::NewDispatchFuncCallGatewayMessage;

Server::Server()
    : engine_conn_port_(-1), http_port_(-1),
      listen_backlog_(kDefaultListenBackLog), num_io_workers_(kDefaultNumIOWorkers),
      next_http_conn_worker_id_(0), next_http_connection_id_(0),
      read_buffer_pool_("HandshakeRead", 128),
      next_call_id_(1), inflight_requests_(0), last_request_timestamp_(-1),
      incoming_requests_stat_(
          stat::Counter::StandardReportCallback("incoming_requests")),
      requests_instant_rps_stat_(
          stat::StatisticsCollector<float>::StandardReportCallback("requests_instant_rps")),
      inflight_requests_stat_(
          stat::StatisticsCollector<uint16_t>::StandardReportCallback("inflight_requests")) {
    UV_DCHECK_OK(uv_tcp_init(uv_loop(), &uv_engine_conn_handle_));
    uv_engine_conn_handle_.data = this;
    UV_DCHECK_OK(uv_tcp_init(uv_loop(), &uv_http_handle_));
    uv_http_handle_.data = this;
}

Server::~Server() {}

void Server::StartInternal() {
    // Load function config file
    CHECK(!func_config_file_.empty());
    CHECK(fs_utils::ReadContents(func_config_file_, &func_config_json_))
        << "Failed to read from file " << func_config_file_;
    CHECK(func_config_.Load(func_config_json_));
    // Start IO workers
    CHECK_GT(num_io_workers_, 0);
    HLOG(INFO) << fmt::format("Start {} IO workers", num_io_workers_);
    for (int i = 0; i < num_io_workers_; i++) {
        auto io_worker = CreateIOWorker(fmt::format("IO-{}", i));
        io_workers_.push_back(io_worker);
    }
    struct sockaddr_in bind_addr;
    CHECK(!address_.empty());
    CHECK_NE(engine_conn_port_, -1);
    CHECK_NE(http_port_, -1);
    // Listen on address:engine_conn_port for engine connections
    UV_CHECK_OK(uv_ip4_addr(address_.c_str(), engine_conn_port_, &bind_addr));
    UV_CHECK_OK(uv_tcp_bind(&uv_http_handle_, (const struct sockaddr *)&bind_addr, 0));
    HLOG(INFO) << fmt::format("Listen on {}:{} for engine connections",
                              address_, engine_conn_port_);
    UV_CHECK_OK(uv_listen(
        UV_AS_STREAM(&uv_engine_conn_handle_), listen_backlog_,
        &Server::EngineConnectionCallback));
    // Listen on address:http_port for HTTP requests
    UV_CHECK_OK(uv_ip4_addr(address_.c_str(), http_port_, &bind_addr));
    UV_CHECK_OK(uv_tcp_bind(&uv_http_handle_, (const struct sockaddr *)&bind_addr, 0));
    HLOG(INFO) << fmt::format("Listen on {}:{} for HTTP requests", address_, http_port_);
    UV_CHECK_OK(uv_listen(
        UV_AS_STREAM(&uv_http_handle_), listen_backlog_,
        &Server::HttpConnectionCallback));
}

void Server::StopInternal() {
    uv_close(UV_AS_HANDLE(&uv_engine_conn_handle_), nullptr);
    uv_close(UV_AS_HANDLE(&uv_http_handle_), nullptr);
}

void Server::OnConnectionClose(server::ConnectionBase* connection) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop());
    if (connection->type() == HttpConnection::kTypeId) {
        absl::MutexLock lk(&mu_);
        DCHECK(connections_.contains(connection->id()));
        connections_.erase(connection->id());
    } else {
        HLOG(ERROR) << "Unknown connection type!";
    }
}

void Server::OnNewHttpFuncCall(HttpConnection* connection, FuncCallContext* func_call_context) {
    auto func_entry = func_config_.find_by_func_name(func_call_context->func_name());
    DCHECK(func_entry != nullptr);
    FuncCall func_call = NewFuncCall(func_entry->func_id, /* client_id= */ 0,
                                     next_call_id_.fetch_add(1));
    func_call_context->set_func_call(func_call);
    {
        absl::MutexLock lk(&mu_);
        running_func_calls_[func_call.full_call_id] = std::make_pair(
            connection->id(), func_call_context);
    }
    server::IOWorker* io_worker = server::IOWorker::current();
    DCHECK(io_worker != nullptr);
    server::ConnectionBase* engine_connection = io_worker->PickRandomConnection(
        EngineConnection::type_id(/* node_id= */ 0));
    GatewayMessage dispatch_message = NewDispatchFuncCallGatewayMessage(func_call);
    dispatch_message.payload_size = func_call_context->input().size();
    engine_connection->as_ptr<EngineConnection>()->SendMessage(
        dispatch_message, func_call_context->input());
}

void Server::OnRecvEngineMessage(EngineConnection* connection, const GatewayMessage& message,
                                 std::span<const char> payload) {
    if (IsFuncCallCompleteMessage(message)
            || IsFuncCallFailedMessage(message)
            || IsFuncCallDiscardedMessage(message)) {
        FuncCall func_call = GetFuncCallFromMessage(message);
        FuncCallContext* func_call_context = nullptr;
        std::shared_ptr<server::ConnectionBase> connection;
        {
            absl::MutexLock lk(&mu_);
            if (running_func_calls_.contains(func_call.full_call_id)) {
                int connection_id = running_func_calls_[func_call.full_call_id].first;
                if (connections_.contains(connection_id)) {
                    connection = connections_[connection_id];
                    func_call_context = running_func_calls_[func_call.full_call_id].second;
                }
                running_func_calls_.erase(func_call.full_call_id);
            }
        }
        if (func_call_context != nullptr) {
            if (IsFuncCallCompleteMessage(message)) {
                func_call_context->set_status(FuncCallContext::kSuccess);
                func_call_context->append_output(payload);
            } else if (IsFuncCallFailedMessage(message) || IsFuncCallDiscardedMessage(message)) {
                func_call_context->set_status(FuncCallContext::kFailed);
            } else {
                HLOG(FATAL) << "Unreachable";
            }
            connection->as_ptr<HttpConnection>()->OnFuncCallFinished(func_call_context);
        }
    } else {
        HLOG(ERROR) << "Unknown engine message type";
    }
}

bool Server::OnEngineHandshake(uv_tcp_t* uv_handle, std::span<const char> data) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop());
    DCHECK_GE(data.size(), sizeof(GatewayMessage));
    const GatewayMessage* message = reinterpret_cast<const GatewayMessage*>(data.data());
    if (!IsEngineHandshakeMessage(*message)) {
        HLOG(ERROR) << "Unexpected engine handshake message";
        return false;
    }
    uint16_t node_id = message->node_id;
    uint16_t conn_id = message->conn_id;
    std::span<const char> remaining_data(data.data() + sizeof(GatewayMessage),
                                         data.size() - sizeof(GatewayMessage));
    std::shared_ptr<server::ConnectionBase> connection(
        new EngineConnection(this, node_id, conn_id, remaining_data));
    size_t& next_worker_id = next_engine_conn_worker_id_[node_id];
    DCHECK_LT(next_worker_id, io_workers_.size());
    server::IOWorker* io_worker = io_workers_[next_worker_id];
    next_worker_id = (next_worker_id + 1) % io_workers_.size();
    RegisterConnection(io_worker, connection.get(), UV_AS_STREAM(uv_handle));
    DCHECK_GE(connection->id(), 0);
    DCHECK(!engine_connections_.contains(connection->id()));
    engine_connections_[connection->id()] = std::move(connection);
    return true;
}

namespace {
static void HandleFreeCallback(uv_handle_t* handle) {
    free(handle);
}
}

UV_CONNECTION_CB_FOR_CLASS(Server, HttpConnection) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to open HTTP connection: " << uv_strerror(status);
        return;
    }
    std::shared_ptr<server::ConnectionBase> connection(
        new HttpConnection(this, next_http_connection_id_++));
    uv_tcp_t* client = reinterpret_cast<uv_tcp_t*>(malloc(sizeof(uv_tcp_t)));
    UV_DCHECK_OK(uv_tcp_init(uv_loop(), client));
    if (uv_accept(UV_AS_STREAM(&uv_http_handle_), UV_AS_STREAM(client)) == 0) {
        DCHECK_LT(next_http_conn_worker_id_, io_workers_.size());
        server::IOWorker* io_worker = io_workers_[next_http_conn_worker_id_];
        next_http_conn_worker_id_ = (next_http_conn_worker_id_ + 1) % io_workers_.size();
        RegisterConnection(io_worker, connection.get(), UV_AS_STREAM(client));
        DCHECK_GE(connection->id(), 0);
        {
            absl::MutexLock lk(&mu_);
            DCHECK(!connections_.contains(connection->id()));
            connections_[connection->id()] = std::move(connection);
        }
    } else {
        LOG(ERROR) << "Failed to accept new HTTP connection";
        free(client);
    }
}

class Server::OngoingEngineHandshake : public uv::Base {
public:
    OngoingEngineHandshake(Server* server, uv_tcp_t* uv_handle);
    ~OngoingEngineHandshake();

private:
    Server* server_;
    uv_tcp_t* uv_handle_;
    utils::AppendableBuffer read_buffer_;

    void OnReadHandshakeMessage();
    void OnReadError();

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadMessage);

    DISALLOW_COPY_AND_ASSIGN(OngoingEngineHandshake);
};

UV_CONNECTION_CB_FOR_CLASS(Server, EngineConnection) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to open engine connection: " << uv_strerror(status);
        return;
    }
    uv_tcp_t* client = reinterpret_cast<uv_tcp_t*>(malloc(sizeof(uv_tcp_t)));
    UV_DCHECK_OK(uv_tcp_init(uv_loop(), client));
    if (uv_accept(UV_AS_STREAM(&uv_engine_conn_handle_), UV_AS_STREAM(client)) == 0) {
        ongoing_engine_handshakes_.insert(std::unique_ptr<OngoingEngineHandshake>(
            new OngoingEngineHandshake(this, client)));
    } else {
        LOG(ERROR) << "Failed to accept new engine connection";
        free(client);
    }
}

Server::OngoingEngineHandshake::OngoingEngineHandshake(Server* server, uv_tcp_t* uv_handle)
    : server_(server), uv_handle_(uv_handle) {
    uv_handle_->data = this;
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_handle_),
                               &OngoingEngineHandshake::BufferAllocCallback,
                               &OngoingEngineHandshake::ReadMessageCallback));
}

Server::OngoingEngineHandshake::~OngoingEngineHandshake() {
    if (uv_handle_ != nullptr) {
        uv_close(UV_AS_HANDLE(uv_handle_), HandleFreeCallback);
    }
}

void Server::OngoingEngineHandshake::OnReadHandshakeMessage() {
    if (server_->OnEngineHandshake(uv_handle_, read_buffer_.to_span())) {
        uv_handle_ = nullptr;
    }
    server_->ongoing_engine_handshakes_.erase(this);
}

void Server::OngoingEngineHandshake::OnReadError() {
    server_->ongoing_engine_handshakes_.erase(this);
}

UV_ALLOC_CB_FOR_CLASS(Server::OngoingEngineHandshake, BufferAlloc) {
    server_->read_buffer_pool_.Get(buf);
}

UV_READ_CB_FOR_CLASS(Server::OngoingEngineHandshake, ReadMessage) {
    auto reclaim_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            server_->read_buffer_pool_.Return(buf);
        }
    });
    if (nread < 0) {
        HLOG(ERROR) << "Read error on handshake: " << uv_strerror(nread);
        OnReadError();
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    read_buffer_.AppendData(buf->base, nread);
    if (read_buffer_.length() >= sizeof(GatewayMessage)) {
        UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(uv_handle_)));
        OnReadHandshakeMessage();
    }
}

}  // namespace gateway
}  // namespace faas
