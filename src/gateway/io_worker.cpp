#include "gateway/io_worker.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

IOWorker::IOWorker(Server* server, int worker_id)
    : server_(server), worker_id_(worker_id), state_(kCreated),
      log_header_(absl::StrFormat("IOWorker[%d]: ", worker_id)),
      event_loop_thread_(absl::StrFormat("IOWorker[%d]_EventLoop", worker_id),
                         std::bind(&IOWorker::EventLoopThreadMain, this)),
      read_buffer_pool_(absl::StrFormat("IOWorker-%d", worker_id), kReadBufferSize) {
    LIBUV_CHECK_OK(uv_loop_init(&uv_loop_));
    uv_loop_.data = &event_loop_thread_;
    LIBUV_CHECK_OK(uv_async_init(&uv_loop_, &stop_event_, &IOWorker::StopCallback));
    stop_event_.data = this;
}

IOWorker::~IOWorker() {
    State state = state_.load();
    CHECK(state == kCreated || state == kStopped);
    CHECK(connections_.empty());
    LIBUV_CHECK_OK(uv_loop_close(&uv_loop_));
}

namespace {
void PipeReadBufferAllocCallback(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
    size_t buf_size = 32;
    buf->base = reinterpret_cast<char*>(malloc(buf_size));
    buf->len = buf_size;
}
}

void IOWorker::Start(int pipe_to_server_fd) {
    CHECK(state_.load() == kCreated);
    LIBUV_CHECK_OK(uv_pipe_init(&uv_loop_, &pipe_to_server_, 1));
    pipe_to_server_.data = this;
    LIBUV_CHECK_OK(uv_pipe_open(&pipe_to_server_, pipe_to_server_fd));
    LIBUV_CHECK_OK(uv_read_start(reinterpret_cast<uv_stream_t*>(&pipe_to_server_),
                                 &PipeReadBufferAllocCallback,
                                 &IOWorker::NewConnectionCallback));
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void IOWorker::ScheduleStop() {
    LIBUV_CHECK_OK(uv_async_send(&stop_event_));
}

void IOWorker::WaitForFinish() {
    CHECK(state_.load() != kCreated);
    event_loop_thread_.Join();
    CHECK(state_.load() == kStopped);
}

void IOWorker::NewReadBuffer(size_t suggested_size, uv_buf_t* buf) {
    CHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    read_buffer_pool_.Get(buf);
}

void IOWorker::ReturnReadBuffer(const uv_buf_t* buf) {
    CHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    read_buffer_pool_.Return(buf);
}

void IOWorker::OnConnectionClose(Connection* connection) {
    CHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    CHECK(connections_.contains(connection));
    connections_.erase(connection);
    HVLOG(1) << "Connection with ID " << connection->id() << " closed, "
             << "current active connections is " << connections_.size();
    uv_write_t* write_req = connection->uv_write_req_for_back_transfer();
    size_t buf_len = sizeof(void*);
    char* buf = connection->pipe_write_buf_for_transfer();
    memcpy(buf, &connection, buf_len);
    uv_buf_t uv_buf = uv_buf_init(buf, buf_len);
    write_req->data = this;
    LIBUV_CHECK_OK(uv_write(write_req, reinterpret_cast<uv_stream_t*>(&pipe_to_server_),
                            &uv_buf, 1, &IOWorker::PipeWriteCallback));
    if (state_.load(std::memory_order_consume) == kStopping && connections_.empty()) {
        // We have returned all Connection objects to Server
        uv_close(reinterpret_cast<uv_handle_t*>(&pipe_to_server_), nullptr);
    }
}

void IOWorker::EventLoopThreadMain() {
    HLOG(INFO) << "Event loop starts";
    int ret = uv_run(&uv_loop_, UV_RUN_DEFAULT);
    if (ret != 0) {
        HLOG(WARNING) << "uv_run returns non-zero value: " << ret;
    }
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

UV_ASYNC_CB_FOR_CLASS(IOWorker, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    HLOG(INFO) << "Start stopping process";
    LIBUV_CHECK_OK(uv_read_stop(reinterpret_cast<uv_stream_t*>(&pipe_to_server_)));
    if (connections_.empty()) {
        uv_close(reinterpret_cast<uv_handle_t*>(&pipe_to_server_), nullptr);
    } else {
        for (Connection* connection : connections_) {
            connection->ScheduleClose();
        }
    }
    uv_close(reinterpret_cast<uv_handle_t*>(&stop_event_), nullptr);
    state_.store(kStopping);
}

UV_READ_CB_FOR_CLASS(IOWorker, NewConnection) {
    CHECK_EQ(nread, static_cast<ssize_t>(sizeof(void*)));
    Connection* connection;
    memcpy(&connection, buf->base, sizeof(void*));
    free(buf->base);
    uv_tcp_t* client = connection->uv_tcp_handle();
    LIBUV_CHECK_OK(uv_tcp_init(&uv_loop_, client));
    LIBUV_CHECK_OK(uv_accept(reinterpret_cast<uv_stream_t*>(&pipe_to_server_),
                             reinterpret_cast<uv_stream_t*>(client)));
    connection->Start(this);
    connections_.insert(connection);
    if (state_.load(std::memory_order_consume) == kStopping) {
        LOG(WARNING) << "Receive new connection in stopping state, will close it directly";
        connection->ScheduleClose();
    } else {
        HVLOG(1) << "Accept new connection with ID " << connection->id() << ", "
                 << "current active connections = " << connections_.size();
    }
}

UV_WRITE_CB_FOR_CLASS(IOWorker, PipeWrite) {
    if (status != 0) {
        HLOG(ERROR) << "Failed to write to pipe: " << uv_strerror(status);
    }
}

}  // namespace gateway
}  // namespace faas
