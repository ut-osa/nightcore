#include "gateway/server.h"

#include "utils/uv_utils.h"
#include <absl/strings/match.h>
#include <absl/strings/strip.h>
#include <absl/strings/numbers.h>

#define HLOG(l) LOG(l) << "Server: "
#define HVLOG(l) VLOG(l) << "Server: "

namespace faas {
namespace gateway {

using protocol::Status;
using protocol::Role;
using protocol::HandshakeMessage;
using protocol::HandshakeResponse;
using protocol::FuncCall;
using protocol::MessageType;
using protocol::Message;

Server::Server()
    : state_(kCreated), port_(-1), listen_backlog_(kDefaultListenBackLog),
      num_http_workers_(kDefaultNumHttpWorkers), num_ipc_workers_(kDefaultNumIpcWorkers),
      event_loop_thread_("Server_EventLoop", std::bind(&Server::EventLoopThreadMain, this)),
      next_http_connection_id_(0), next_http_worker_id_(0), next_ipc_worker_id_(0),
      next_client_id_(1), next_call_id_(0) {
    UV_CHECK_OK(uv_loop_init(&uv_loop_));
    uv_loop_.data = &event_loop_thread_;
    UV_CHECK_OK(uv_tcp_init(&uv_loop_, &uv_tcp_handle_));
    uv_tcp_handle_.data = this;
    UV_CHECK_OK(uv_async_init(&uv_loop_, &stop_event_, &Server::StopCallback));
    stop_event_.data = this;
}

Server::~Server() {
    State state = state_.load();
    CHECK(state == kCreated || state == kStopped);
    UV_CHECK_OK(uv_loop_close(&uv_loop_));
}

void Server::RegisterInternalRequestHandlers() {
    // POST /shutdown
    RegisterSyncRequestHandler([] (absl::string_view method, absl::string_view path) -> bool {
        return method == "POST" && path == "/shutdown";
    }, [this] (HttpSyncRequestContext* context) {
        context->AppendStrToResponseBody("Server is shutting down\n");
        ScheduleStop();
    });
    // GET /hello
    RegisterSyncRequestHandler([] (absl::string_view method, absl::string_view path) -> bool {
        return method == "GET" && path == "/hello";
    }, [] (HttpSyncRequestContext* context) {
        context->AppendStrToResponseBody("Hello world\n");
    });
    // POST /function/[func_id]
    RegisterAsyncRequestHandler([] (absl::string_view method, absl::string_view path) -> bool {
        if (method != "POST" || !absl::StartsWith(path, "/function/")) {
            return false;
        }
        int func_id;
        if (!SimpleAtoi(absl::StripPrefix(path, "/function/"), &func_id)) {
            return false;
        }
        return func_id > 0;
    }, [this] (std::shared_ptr<HttpAsyncRequestContext> context) {
        int func_id;
        CHECK(SimpleAtoi(absl::StripPrefix(context->path(), "/function/"), &func_id));
        CHECK(func_id > 0);
        OnExternalFuncCall(static_cast<uint16_t>(func_id), std::move(context));
    });
}

void Server::Start() {
    CHECK(state_.load() == kCreated);
    RegisterInternalRequestHandlers();
    // Start IO workers
    for (int i = 0; i < num_http_workers_; i++) {
        auto io_worker = absl::make_unique<IOWorker>(this, absl::StrFormat("HttpWorker-%d", i),
                                                     kHttpConnectionBufferSize);
        InitAndStartIOWorker(io_worker.get());
        http_workers_.push_back(io_worker.get());
        io_workers_.push_back(std::move(io_worker));
    }
    for (int i = 0; i < num_ipc_workers_; i++) {
        auto io_worker = absl::make_unique<IOWorker>(this, absl::StrFormat("IpcWorker-%d", i),
                                                     kMessageConnectionBufferSize,
                                                     kMessageConnectionBufferSize);
        InitAndStartIOWorker(io_worker.get());
        ipc_workers_.push_back(io_worker.get());
        io_workers_.push_back(std::move(io_worker));
    }
    // Listen on address:port
    struct sockaddr_in bind_addr;
    CHECK(!address_.empty());
    CHECK_NE(port_, -1);
    UV_CHECK_OK(uv_ip4_addr(address_.c_str(), port_, &bind_addr));
    UV_CHECK_OK(uv_tcp_bind(&uv_tcp_handle_, (const struct sockaddr *)&bind_addr, 0));
    HLOG(INFO) << "Listen on " << address_ << ":" << port_;
    UV_CHECK_OK(uv_listen(
        reinterpret_cast<uv_stream_t*>(&uv_tcp_handle_), listen_backlog_,
        &Server::HttpConnectionCallback));
    // Listen on ipc_path
    UV_CHECK_OK(uv_pipe_init(&uv_loop_, &uv_ipc_handle_, 0));
    uv_ipc_handle_.data = this;
    unlink(ipc_path_.c_str());
    UV_CHECK_OK(uv_pipe_bind(&uv_ipc_handle_, ipc_path_.c_str()));
    HLOG(INFO) << "Listen on " << ipc_path_ << " for IPC with watchdog processes";
    UV_CHECK_OK(uv_listen(
        reinterpret_cast<uv_stream_t*>(&uv_ipc_handle_), listen_backlog_,
        &Server::MessageConnectionCallback));
    // Start thread for running event loop
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void Server::ScheduleStop() {
    HLOG(INFO) << "Scheduled to stop";
    UV_CHECK_OK(uv_async_send(&stop_event_));
}

void Server::WaitForFinish() {
    CHECK(state_.load() != kCreated);
    for (const auto& io_worker : io_workers_) {
        io_worker->WaitForFinish();
    }
    event_loop_thread_.Join();
    CHECK(state_.load() == kStopped);
    HLOG(INFO) << "Stopped";
}

void Server::RegisterSyncRequestHandler(RequestMatcher matcher, SyncRequestHandler handler) {
    CHECK(state_.load() == kCreated);
    request_handlers_.emplace_back(new RequestHandler(std::move(matcher), std::move(handler)));
}

void Server::RegisterAsyncRequestHandler(RequestMatcher matcher, AsyncRequestHandler handler) {
    CHECK(state_.load() == kCreated);
    request_handlers_.emplace_back(new RequestHandler(std::move(matcher), std::move(handler)));
}

bool Server::MatchRequest(absl::string_view method, absl::string_view path,
                          const RequestHandler** request_handler) const {
    for (const std::unique_ptr<RequestHandler>& entry : request_handlers_) {
        if (entry->matcher_(method, path)) {
            *request_handler = entry.get();
            return true;
        }
    }
    return false;
}

void Server::EventLoopThreadMain() {
    HLOG(INFO) << "Event loop starts";
    int ret = uv_run(&uv_loop_, UV_RUN_DEFAULT);
    if (ret != 0) {
        HLOG(WARNING) << "uv_run returns non-zero value: " << ret;
    }
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

IOWorker* Server::PickHttpWorker() {
    IOWorker* io_worker = http_workers_[next_http_worker_id_];
    next_http_worker_id_ = (next_http_worker_id_ + 1) % http_workers_.size();
    return io_worker;
}

IOWorker* Server::PickIpcWorker() {
    IOWorker* io_worker = ipc_workers_[next_ipc_worker_id_];
    next_ipc_worker_id_ = (next_ipc_worker_id_ + 1) % ipc_workers_.size();
    return io_worker;
}

namespace {
void PipeReadBufferAllocCallback(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
    size_t buf_size = 256;
    buf->base = reinterpret_cast<char*>(malloc(buf_size));
    buf->len = buf_size;
}
}

void Server::InitAndStartIOWorker(IOWorker* io_worker) {
    int pipe_fd_for_worker;
    pipes_to_io_worker_[io_worker] = CreatePipeToWorker(&pipe_fd_for_worker);
    uv_pipe_t* pipe_to_worker = pipes_to_io_worker_[io_worker].get();
    UV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(pipe_to_worker),
                              &PipeReadBufferAllocCallback,
                              &Server::ReturnConnectionCallback));
    io_worker->Start(pipe_fd_for_worker);
}

std::unique_ptr<uv_pipe_t> Server::CreatePipeToWorker(int* pipe_fd_for_worker) {
    int pipe_fds[2];
    // pipe2 does not work with uv_write2, use socketpair instead
    CHECK_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, pipe_fds), 0);
    std::unique_ptr<uv_pipe_t> pipe_to_worker = absl::make_unique<uv_pipe_t>();
    UV_CHECK_OK(uv_pipe_init(&uv_loop_, pipe_to_worker.get(), 1));
    pipe_to_worker->data = this;
    UV_CHECK_OK(uv_pipe_open(pipe_to_worker.get(), pipe_fds[0]));
    *pipe_fd_for_worker = pipe_fds[1];
    return pipe_to_worker;
}

void Server::TransferConnectionToWorker(IOWorker* io_worker, Connection* connection,
                                        uv_stream_t* send_handle) {
    CHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    uv_write_t* write_req = connection->uv_write_req_for_transfer();
    size_t buf_len = sizeof(void*);
    char* buf = connection->pipe_write_buf_for_transfer();
    memcpy(buf, &connection, buf_len);
    uv_buf_t uv_buf = uv_buf_init(buf, buf_len);
    uv_pipe_t* pipe_to_worker = pipes_to_io_worker_[io_worker].get();
    UV_CHECK_OK(uv_write2(write_req, reinterpret_cast<uv_stream_t*>(pipe_to_worker),
                          &uv_buf, 1, reinterpret_cast<uv_stream_t*>(send_handle),
                          &PipeWrite2Callback));
    uv_close(reinterpret_cast<uv_handle_t*>(send_handle), nullptr);
}

void Server::ReturnConnection(Connection* connection) {
    CHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    if (connection->type() == Connection::Type::Http) {
        HttpConnection* http_connection = static_cast<HttpConnection*>(connection);
        free_http_connections_.push_back(http_connection);
        active_http_connections_.erase(http_connection);
        HLOG(INFO) << "HttpConnection with ID " << http_connection->id() << " is returned, "
                   << "free connection count is " << free_http_connections_.size() << ", "
                   << "active connection is " << active_http_connections_.size();
    } else if (connection->type() == Connection::Type::Message) {
        MessageConnection* message_connection = static_cast<MessageConnection*>(connection);
        {
            // absl::MutexLock lk(&message_connection_mu_);
            CHECK(message_connections_by_client_id_.contains(message_connection->client_id()));
            message_connections_by_client_id_.erase(message_connection->client_id());
            if (message_connection->role() == Role::WATCHDOG) {
                CHECK(watchdog_connections_by_func_id_.contains(message_connection->func_id()));
                watchdog_connections_by_func_id_.erase(message_connection->func_id());
            }
            message_connections_.erase(message_connection);
        }
    } else {
        LOG(FATAL) << "Unknown connection type!";
    }
}

void Server::OnNewHandshake(MessageConnection* connection,
                            const HandshakeMessage& message, HandshakeResponse* response) {
    uint16_t client_id = next_client_id_.fetch_add(1);
    response->status = static_cast<uint16_t>(Status::OK);
    response->client_id = client_id;
    {
        absl::MutexLock lk(&message_connection_mu_);
        message_connections_by_client_id_[client_id] = connection;
        if (static_cast<Role>(message.role) == Role::WATCHDOG) {
            watchdog_connections_by_func_id_[message.func_id] = connection;
        }
    }
}

void Server::OnRecvMessage(MessageConnection* connection, const Message& message) {
    MessageType type = static_cast<MessageType>(message.message_type);
    if (type == MessageType::INVOKE_FUNC) {
        uint16_t func_id = message.func_call.func_id;
        absl::MutexLock lk(&message_connection_mu_);
        if (watchdog_connections_by_func_id_.contains(func_id)) {
            MessageConnection* connection = watchdog_connections_by_func_id_[func_id];
            connection->WriteMessage({
                .message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC),
                .func_call = message.func_call
            });
        } else {
            HLOG(WARNING) << "Cannot find message connection of watchdog with func_id " << func_id;
        }
    } else if (type == MessageType::FUNC_CALL_COMPLETE) {
        uint16_t client_id = message.func_call.client_id;
        if (client_id > 0) {
            absl::MutexLock lk(&message_connection_mu_);
            if (message_connections_by_client_id_.contains(client_id)) {
                MessageConnection* connection = message_connections_by_client_id_[client_id];
                connection->WriteMessage({
                    .message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE),
                    .func_call = message.func_call
                });
            } else {
                HLOG(WARNING) << "Cannot find message connection with client_id " << client_id;
            }
        } else {
            absl::MutexLock lk(&external_func_calls_mu_);
            uint64_t full_call_id = message.func_call.full_call_id;
            if (external_func_calls_.contains(full_call_id)) {
                auto request_context = external_func_calls_[full_call_id];
                external_func_calls_.erase(full_call_id);
                request_context->AppendStrToResponseBody("Finish function call\n");
                request_context->Finish();
            } else {
                HLOG(WARNING) << "Cannot find external call with func_id=" << message.func_call.call_id << ", "
                              << "call_id=" << message.func_call.call_id;
            }
        }
    } else {
        LOG(ERROR) << "Unknown message type!";
    }
}

void Server::OnExternalFuncCall(uint16_t func_id, std::shared_ptr<HttpAsyncRequestContext> request_context) {
    FuncCall call;
    call.func_id = func_id;
    call.client_id = 0;
    call.call_id = next_call_id_.fetch_add(1);
    {
        absl::MutexLock lk(&message_connection_mu_);
        if (watchdog_connections_by_func_id_.contains(func_id)) {
            MessageConnection* connection = watchdog_connections_by_func_id_[func_id];
            connection->WriteMessage({
                .message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC),
                .func_call = call
            });
        } else {
            request_context->AppendStrToResponseBody(
                absl::StrFormat("Cannot find function with func_id %d\n", static_cast<int>(func_id)));
            request_context->SetStatus(404);
            request_context->Finish();
            return;
        }
    }
    {
        absl::MutexLock lk(&external_func_calls_mu_);
        external_func_calls_[call.full_call_id] = std::move(request_context);
    }
}

UV_CONNECTION_CB_FOR_CLASS(Server, HttpConnection) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to open HTTP connection: " << uv_strerror(status);
        return;
    }
    HttpConnection* connection;
    if (!free_http_connections_.empty()) {
        connection = free_http_connections_.back();
        free_http_connections_.pop_back();
        connection->Reset(next_http_connection_id_++);
    } else {
        http_connections_.push_back(absl::make_unique<HttpConnection>(this, next_http_connection_id_++));
        connection = http_connections_.back().get();
        HLOG(INFO) << "Allocate new HttpConnection object, current count is " << http_connections_.size();
    }
    uv_tcp_t client;
    UV_CHECK_OK(uv_tcp_init(&uv_loop_, &client));
    UV_CHECK_OK(uv_accept(reinterpret_cast<uv_stream_t*>(&uv_tcp_handle_),
                          reinterpret_cast<uv_stream_t*>(&client)));
    TransferConnectionToWorker(PickHttpWorker(), connection, reinterpret_cast<uv_stream_t*>(&client));
    active_http_connections_.insert(connection);
}

UV_CONNECTION_CB_FOR_CLASS(Server, MessageConnection) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to open message connection: " << uv_strerror(status);
        return;
    }
    HLOG(INFO) << "New message connection";
    std::unique_ptr<MessageConnection> connection = absl::make_unique<MessageConnection>(this);
    uv_pipe_t client;
    UV_CHECK_OK(uv_pipe_init(&uv_loop_, &client, 0));
    UV_CHECK_OK(uv_accept(reinterpret_cast<uv_stream_t*>(&uv_ipc_handle_),
                          reinterpret_cast<uv_stream_t*>(&client)));
    TransferConnectionToWorker(PickIpcWorker(), connection.get(),
                               reinterpret_cast<uv_stream_t*>(&client));
    message_connections_.insert(std::move(connection));
}

UV_READ_CB_FOR_CLASS(Server, ReturnConnection) {
    if (nread < 0) {
        if (nread == UV_EOF) {
            HLOG(WARNING) << "Pipe is closed by the corresponding IO worker";
        } else {
            HLOG(ERROR) << "Failed to read from pipe: " << uv_strerror(nread);
        }
    }
    if (nread <= 0) return;
    utils::ReadMessages<Connection*>(
        &return_connection_read_buffer_, buf->base, nread,
        [this] (Connection** connection) {
            ReturnConnection(*connection);
        });
    free(buf->base);
}

UV_ASYNC_CB_FOR_CLASS(Server, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    HLOG(INFO) << "Start stopping process";
    for (const auto& io_worker : io_workers_) {
        io_worker->ScheduleStop();
        uv_pipe_t* pipe = pipes_to_io_worker_[io_worker.get()].get();
        UV_CHECK_OK(uv_read_stop(reinterpret_cast<uv_stream_t*>(pipe)));
        uv_close(reinterpret_cast<uv_handle_t*>(pipe), nullptr);
    }
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_tcp_handle_), nullptr);
    uv_close(reinterpret_cast<uv_handle_t*>(&uv_ipc_handle_), nullptr);
    uv_close(reinterpret_cast<uv_handle_t*>(&stop_event_), nullptr);
    state_.store(kStopping);
}

UV_WRITE_CB_FOR_CLASS(Server, PipeWrite2) {
    CHECK(status == 0) << "Failed to write to pipe: " << uv_strerror(status);
}

}  // namespace gateway
}  // namespace faas
