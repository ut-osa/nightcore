#include "server/server_base.h"

#define HLOG(l) LOG(l) << "Server: "
#define HVLOG(l) VLOG(l) << "Server: "

namespace faas {
namespace server {

ServerBase::ServerBase()
    : state_(kCreated),
      event_loop_thread_("Server/EL", absl::bind_front(&ServerBase::EventLoopThreadMain, this)),
      next_connection_id_(0) {
    UV_DCHECK_OK(uv_loop_init(&uv_loop_));
    uv_loop_.data = &event_loop_thread_;
    UV_DCHECK_OK(uv_async_init(&uv_loop_, &stop_event_, &ServerBase::StopCallback));
    stop_event_.data = this;
}


ServerBase::~ServerBase() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
    UV_DCHECK_OK(uv_loop_close(&uv_loop_));
}

void ServerBase::Start() {
    DCHECK(state_.load() == kCreated);
    StartInternal();
    // Start thread for running event loop
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void ServerBase::ScheduleStop() {
    HLOG(INFO) << "Scheduled to stop";
    UV_DCHECK_OK(uv_async_send(&stop_event_));
}

void ServerBase::WaitForFinish() {
    DCHECK(state_.load() != kCreated);
    for (const auto& io_worker : io_workers_) {
        io_worker->WaitForFinish();
    }
    event_loop_thread_.Join();
    DCHECK(state_.load() == kStopped);
    HLOG(INFO) << "Stopped";
}

void ServerBase::EventLoopThreadMain() {
    HLOG(INFO) << "Event loop starts";
    int ret = uv_run(&uv_loop_, UV_RUN_DEFAULT);
    if (ret != 0) {
        HLOG(WARNING) << "uv_run returns non-zero value: " << ret;
    }
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

namespace {
static void PipeReadBufferAllocCallback(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
    size_t buf_size = 256;
    buf->base = reinterpret_cast<char*>(malloc(buf_size));
    buf->len = buf_size;
}
}

IOWorker* ServerBase::CreateIOWorker(std::string_view worker_name,
                                     size_t read_buffer_size, size_t write_buffer_size) {
    DCHECK(state_.load() == kCreated);
    auto io_worker = std::make_unique<IOWorker>(worker_name, read_buffer_size, write_buffer_size);
    int pipe_fds[2] = { -1, -1 };
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pipe_fds) < 0) {
        PLOG(FATAL) << "socketpair failed";
    }
    uv_pipe_t* pipe_to_worker = new uv_pipe_t;
    UV_CHECK_OK(uv_pipe_init(&uv_loop_, pipe_to_worker, 1));
    pipe_to_worker->data = this;
    UV_CHECK_OK(uv_pipe_open(pipe_to_worker, pipe_fds[0]));
    UV_CHECK_OK(uv_read_start(UV_AS_STREAM(pipe_to_worker),
                              &PipeReadBufferAllocCallback,
                              &ServerBase::ReturnConnectionCallback));
    io_worker->Start(pipe_fds[1]);
    pipes_to_io_worker_[io_worker.get()] = std::unique_ptr<uv_pipe_t>(pipe_to_worker);
    IOWorker* ret = io_worker.get();
    io_workers_.insert(std::move(io_worker));
    return ret;
}

void ServerBase::RegisterConnection(IOWorker* io_worker, ConnectionBase* connection,
                                    uv_stream_t* uv_handle) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    uv_write_t* write_req = connection->uv_write_req_for_transfer();
    void** buf = reinterpret_cast<void**>(connection->pipe_write_buf_for_transfer());
    *buf = connection;
    uv_buf_t uv_buf = uv_buf_init(reinterpret_cast<char*>(buf), sizeof(void*));
    uv_pipe_t* pipe_to_worker = pipes_to_io_worker_[io_worker].get();
    write_req->data = uv_handle;
    connection->set_id(next_connection_id_++);
    UV_DCHECK_OK(uv_write2(write_req, UV_AS_STREAM(pipe_to_worker),
                           &uv_buf, 1, UV_AS_STREAM(uv_handle),
                           &PipeWrite2Callback));
}

UV_ASYNC_CB_FOR_CLASS(ServerBase, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    HLOG(INFO) << "Start stopping process";
    for (const auto& io_worker : io_workers_) {
        io_worker->ScheduleStop();
        uv_pipe_t* pipe = pipes_to_io_worker_[io_worker.get()].get();
        UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(pipe)));
        uv_close(UV_AS_HANDLE(pipe), nullptr);
    }
    uv_close(UV_AS_HANDLE(&stop_event_), nullptr);
    StopInternal();
    state_.store(kStopping);
}

UV_READ_CB_FOR_CLASS(ServerBase, ReturnConnection) {
    if (nread < 0) {
        if (nread == UV_EOF) {
            HLOG(WARNING) << "Pipe is closed by the corresponding IO worker";
        } else {
            HLOG(ERROR) << "Failed to read from pipe: " << uv_strerror(nread);
        }
    } else if (nread > 0) {
        utils::ReadMessages<ConnectionBase*>(
            &return_connection_read_buffer_, buf->base, nread,
            [this] (ConnectionBase** connection) {
                OnConnectionClose(*connection);
            });
    }
    free(buf->base);
}

UV_WRITE_CB_FOR_CLASS(ServerBase, PipeWrite2) {
    DCHECK(status == 0) << "Failed to write to pipe: " << uv_strerror(status);
    uv_close(UV_AS_HANDLE(req->data), uv::HandleFreeCallback);
}

}  // namespace server
}  // namespace faas
