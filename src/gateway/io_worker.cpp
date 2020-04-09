#include "gateway/io_worker.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace gateway {

constexpr size_t IOWorker::kDefaultBufferSize;

IOWorker::IOWorker(Server* server, std::string_view worker_name,
                   size_t read_buffer_size, size_t write_buffer_size)
    : server_(server), worker_name_(worker_name), state_(kCreated),
      log_header_(absl::StrFormat("%s: ", worker_name)),
      event_loop_thread_(absl::StrFormat("%s/EL", worker_name),
                         absl::bind_front(&IOWorker::EventLoopThreadMain, this)),
      read_buffer_pool_(absl::StrFormat("%s_Read", worker_name), read_buffer_size),
      write_buffer_pool_(absl::StrFormat("%s_Write", worker_name), write_buffer_size),
      async_event_recv_timestamp_(0),
      bytes_per_read_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          absl::StrFormat("[%s] bytes_per_read", worker_name))),
      write_size_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          absl::StrFormat("[%s] write_size_stat", worker_name))),
      uv_async_delay_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          absl::StrFormat("[%s] uv_async_delay", worker_name))) {
    UV_DCHECK_OK(uv_loop_init(&uv_loop_));
    uv_loop_.data = &event_loop_thread_;
    UV_DCHECK_OK(uv_async_init(&uv_loop_, &stop_event_, &IOWorker::StopCallback));
    stop_event_.data = this;
    UV_DCHECK_OK(uv_async_init(&uv_loop_, &run_fn_event_,
                               &IOWorker::RunScheduledFunctionsCallback));
    run_fn_event_.data = this;
}

IOWorker::~IOWorker() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
    DCHECK(connections_.empty());
    UV_DCHECK_OK(uv_loop_close(&uv_loop_));
}

namespace {
void PipeReadBufferAllocCallback(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
    size_t buf_size = 32;
    buf->base = reinterpret_cast<char*>(malloc(buf_size));
    buf->len = buf_size;
}
}

void IOWorker::Start(int pipe_to_server_fd) {
    DCHECK(state_.load() == kCreated);
    UV_DCHECK_OK(uv_pipe_init(&uv_loop_, &pipe_to_server_, 1));
    pipe_to_server_.data = this;
    UV_DCHECK_OK(uv_pipe_open(&pipe_to_server_, pipe_to_server_fd));
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&pipe_to_server_),
                               &PipeReadBufferAllocCallback,
                               &IOWorker::NewConnectionCallback));
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void IOWorker::ScheduleStop() {
    UV_DCHECK_OK(uv_async_send(&stop_event_));
}

void IOWorker::WaitForFinish() {
    DCHECK(state_.load() != kCreated);
    event_loop_thread_.Join();
    DCHECK(state_.load() == kStopped);
}

void IOWorker::NewReadBuffer(size_t suggested_size, uv_buf_t* buf) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    read_buffer_pool_.Get(buf);
}

void IOWorker::ReturnReadBuffer(const uv_buf_t* buf) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    read_buffer_pool_.Return(buf);
}

void IOWorker::NewWriteBuffer(uv_buf_t* buf) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    write_buffer_pool_.Get(buf);
}

void IOWorker::ReturnWriteBuffer(char* buf) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    write_buffer_pool_.Return(buf);
}

uv_write_t* IOWorker::NewWriteRequest() {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    return write_req_pool_.Get();
}

void IOWorker::ReturnWriteRequest(uv_write_t* write_req) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    write_req_pool_.Return(write_req);
}

void IOWorker::ScheduleFunction(Connection* owner, std::function<void()> fn) {
    if (state_.load(std::memory_order_consume) != kRunning) {
        HLOG(WARNING) << "Cannot schedule function in non-running state, will ignore it";
        return;
    }
    bool within_my_event_loop = uv::WithinEventLoop(&uv_loop_);
    std::unique_ptr<ScheduledFunction> function = std::make_unique<ScheduledFunction>();
    function->owner = owner;
    function->fn = fn;
    {
        absl::MutexLock lk(&scheduled_function_mu_);
        scheduled_functions_.push_back(std::move(function));
        if (!within_my_event_loop) {
#ifdef __FAAS_ENABLE_PROFILING
            uint64_t empty = 0;
            async_event_recv_timestamp_.compare_exchange_strong(
                empty, GetMonotonicMicroTimestamp());
#endif
            UV_DCHECK_OK(uv_async_send(&run_fn_event_));
        }
    }
    if (within_my_event_loop) {
        OnRunScheduledFunctions();
    }
}

void IOWorker::OnConnectionClose(Connection* connection) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    DCHECK(pipe_to_server_.loop == &uv_loop_);
    DCHECK(connections_.contains(connection));
    HLOG(INFO) << "An associated connection closed";
    uv_write_t* write_req = connection->uv_write_req_for_back_transfer();
    size_t buf_len = sizeof(void*);
    char* buf = connection->pipe_write_buf_for_transfer();
    memcpy(buf, &connection, buf_len);
    uv_buf_t uv_buf = uv_buf_init(buf, buf_len);
    write_req->data = connection;
    UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(&pipe_to_server_),
                          &uv_buf, 1, &IOWorker::PipeWriteCallback));
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
    UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(&pipe_to_server_)));
    if (connections_.empty()) {
        HLOG(INFO) << "Close pipe to Server";
        uv_close(UV_AS_HANDLE(&pipe_to_server_), nullptr);
    } else {
        for (Connection* connection : connections_) {
            connection->ScheduleClose();
        }
    }
    uv_close(UV_AS_HANDLE(&stop_event_), nullptr);
    uv_close(UV_AS_HANDLE(&run_fn_event_), nullptr);
    state_.store(kStopping);
}

UV_READ_CB_FOR_CLASS(IOWorker, NewConnection) {
    DCHECK_EQ(nread, gsl::narrow_cast<ssize_t>(sizeof(void*)));
    Connection* connection;
    memcpy(&connection, buf->base, sizeof(void*));
    free(buf->base);
    uv_stream_t* client = connection->InitUVHandle(&uv_loop_);
    UV_DCHECK_OK(uv_accept(UV_AS_STREAM(&pipe_to_server_), client));
    connection->Start(this);
    connections_.insert(connection);
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Receive new connection in stopping state, will close it directly";
        connection->ScheduleClose();
    }
}

UV_WRITE_CB_FOR_CLASS(IOWorker, PipeWrite) {
    DCHECK(status == 0) << "Failed to write to pipe: " << uv_strerror(status);
    Connection* connection = reinterpret_cast<Connection*>(req->data);
    DCHECK(connections_.contains(connection));
    connections_.erase(connection);
    if (state_.load(std::memory_order_consume) == kStopping && connections_.empty()) {
        // We have returned all Connection objects to Server
        HLOG(INFO) << "Close pipe to Server";
        uv_close(UV_AS_HANDLE(&pipe_to_server_), nullptr);
    }
}

UV_ASYNC_CB_FOR_CLASS(IOWorker, RunScheduledFunctions) {
    if (state_.load(std::memory_order_consume) != kRunning) {
        return;
    }
#ifdef __FAAS_ENABLE_PROFILING
    uint64_t async_event_recv_timestamp = async_event_recv_timestamp_.fetch_and(0);
    if (async_event_recv_timestamp != 0) {
        uv_async_delay_stat_.AddSample(GetMonotonicMicroTimestamp() - async_event_recv_timestamp);
    }
#endif
    absl::InlinedVector<std::unique_ptr<ScheduledFunction>, 32> functions;
    {
        absl::MutexLock lk(&scheduled_function_mu_);
        functions = std::move(scheduled_functions_);
        scheduled_functions_.clear();
    }
    for (const auto& function : functions) {
        if (connections_.contains(function->owner)) {
            function->fn();
        } else {
            HLOG(WARNING) << "Owner connection has closed";
        }
    }
}

}  // namespace gateway
}  // namespace faas
