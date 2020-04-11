#include "worker/v2/func_worker.h"

#include "base/thread.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace worker_v2 {

constexpr int FuncWorker::kDefaultWorkerThreadNumber;
constexpr size_t FuncWorker::kBufferSize;

FuncWorker::FuncWorker()
    : log_header_("FuncWorker: "),
      num_worker_threads_(kDefaultWorkerThreadNumber),
      buffer_pool_("FuncWorker", kBufferSize),
      idle_worker_count_(0) {
    if (manager_.is_grpc_service()) {
        HLOG(FATAL) << "func_worker_v2 has not implemented gRPC support";
    }
    manager_.SetIncomingFuncCallCallback(
        absl::bind_front(&FuncWorker::OnIncomingFuncCall, this));
    manager_.SetOutcomingFuncCallCompleteCallback(
        absl::bind_front(&FuncWorker::OnOutcomingFuncCallComplete, this));
    UV_CHECK_OK(uv_loop_init(&uv_loop_));
    UV_CHECK_OK(uv_pipe_init(&uv_loop_, &gateway_ipc_handle_, 0));
    gateway_ipc_handle_.data = this;
    UV_CHECK_OK(uv_pipe_init(&uv_loop_, &watchdog_input_pipe_handle_, 0));
    watchdog_input_pipe_handle_.data = this;
    UV_CHECK_OK(uv_pipe_init(&uv_loop_, &watchdog_output_pipe_handle_, 0));
    watchdog_output_pipe_handle_.data = this;
    UV_CHECK_OK(uv_async_init(&uv_loop_, &new_data_to_send_event_,
                              &FuncWorker::NewDataToSendCallback));
    new_data_to_send_event_.data = this;
}

FuncWorker::~FuncWorker() {}

void FuncWorker::Serve() {
    CHECK(!func_library_path_.empty());
    func_library_ = utils::DynamicLibrary::Create(func_library_path_);
    init_fn_ = func_library_->LoadSymbol<faas_init_fn_t>("faas_init");
    create_func_worker_fn_ = func_library_->LoadSymbol<faas_create_func_worker_fn_t>(
        "faas_create_func_worker");
    destroy_func_worker_fn_ = func_library_->LoadSymbol<faas_destroy_func_worker_fn_t>(
        "faas_destroy_func_worker");
    func_call_fn_ = func_library_->LoadSymbol<faas_func_call_fn_t>(
        "faas_func_call");
    CHECK(init_fn_() == 0) << "Failed to initialize loaded library";

    {
        absl::MutexLock lk(&mu_);
        uv_pipe_connect(&gateway_connect_req_, &gateway_ipc_handle_,
                        std::string(manager_.gateway_ipc_path()).c_str(),
                        &FuncWorker::GatewayConnectCallback);
    }

    CHECK_GT(num_worker_threads_, 0);
    HLOG(INFO) << "Create " << num_worker_threads_
               << " worker threads for running function handler";
    for (int i = 0; i < num_worker_threads_; i++) {
        worker_threads_.push_back(std::make_unique<WorkerThread>(this, i));
    }

    base::Thread::current()->MarkThreadCategory("IO");
    uv_loop_.data = base::Thread::current();
    HLOG(INFO) << "Event loop starts";
    int ret = uv_run(&uv_loop_, UV_RUN_DEFAULT);
    if (ret != 0) {
        HLOG(WARNING) << "uv_run returns non-zero value: " << ret;
    }
    HLOG(INFO) << "Event loop finishes";
}

UV_ALLOC_CB_FOR_CLASS(FuncWorker, BufferAlloc) {
    buffer_pool_.Get(buf);
}

UV_CONNECT_CB_FOR_CLASS(FuncWorker, GatewayConnect) {
    HLOG(INFO) << "Connected to gateway";
    absl::MutexLock lk(&mu_);
    UV_CHECK_OK(uv_pipe_open(&watchdog_input_pipe_handle_,
                             manager_.watchdog_input_pipe_fd()));
    UV_CHECK_OK(uv_pipe_open(&watchdog_output_pipe_handle_,
                             manager_.watchdog_output_pipe_fd()));
    UV_CHECK_OK(uv_read_start(UV_AS_STREAM(&gateway_ipc_handle_),
                              &FuncWorker::BufferAllocCallback,
                              &FuncWorker::RecvGatewayDataCallback));
    UV_CHECK_OK(uv_read_start(UV_AS_STREAM(&watchdog_input_pipe_handle_),
                              &FuncWorker::BufferAllocCallback,
                              &FuncWorker::RecvWatchdogDataCallback));
    manager_.SetSendGatewayDataCallback([this] (std::span<const char> data) {
        {
            absl::MutexLock lk(&gateway_send_buffer_mu_);
            gateway_send_buffer_.AppendData(data);
        }
        SignalNewDataToSend();
    });
    manager_.SetSendWatchdogDataCallback([this] (std::span<const char> data) {
        {
            absl::MutexLock lk(&watchdog_send_buffer_mu_);
            watchdog_send_buffer_.AppendData(data);
        }
        SignalNewDataToSend();
    });
    manager_.Start();
}

UV_READ_CB_FOR_CLASS(FuncWorker, RecvGatewayData) {
    auto reclaim_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            buffer_pool_.Return(buf);
        }
    });
    if (nread < 0) {
        absl::MutexLock lk(&mu_);
        manager_.OnGatewayIOError(uv_strerror(nread));
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    absl::MutexLock lk(&mu_);
    manager_.OnRecvGatewayData(std::span<const char>(buf->base, nread));
}

UV_READ_CB_FOR_CLASS(FuncWorker, RecvWatchdogData) {
    auto reclaim_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            buffer_pool_.Return(buf);
        }
    });
    if (nread < 0) {
        absl::MutexLock lk(&mu_);
        manager_.OnWatchdogIOError(uv_strerror(nread));
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    absl::MutexLock lk(&mu_);
    manager_.OnRecvWatchdogData(std::span<const char>(buf->base, nread));
}

UV_ASYNC_CB_FOR_CLASS(FuncWorker, NewDataToSend) {
    {
        absl::MutexLock lk(&gateway_send_buffer_mu_);
        if (gateway_send_buffer_.length() > 0) {
            SendData(&gateway_ipc_handle_, gateway_send_buffer_.to_span());
            gateway_send_buffer_.Reset();
        }
    }
    {
        absl::MutexLock lk(&watchdog_send_buffer_mu_);
        if (watchdog_send_buffer_.length() > 0) {
            SendData(&watchdog_output_pipe_handle_, watchdog_send_buffer_.to_span());
            watchdog_send_buffer_.Reset();
        }
    }
}

UV_WRITE_CB_FOR_CLASS(FuncWorker, DataSent) {
    auto reclaim_resource = gsl::finally([this, req] {
        buffer_pool_.Return(reinterpret_cast<char*>(req->data));
        write_req_pool_.Return(req);
    });
    if (status != 0) {
        absl::MutexLock lk(&mu_);
        if (req->handle == UV_AS_STREAM(&gateway_ipc_handle_)) {
            manager_.OnGatewayIOError(uv_strerror(status));
        } else {
            manager_.OnWatchdogIOError(uv_strerror(status));
        }
    }
}

class FuncWorker::WorkerThread {
public:
    WorkerThread(FuncWorker* func_worker, int id);
    ~WorkerThread() {}

    void ThreadMain();

private:
    std::string log_header_;
    FuncWorker* func_worker_;
    base::Thread thread_;
    utils::AppendableBuffer output_buffer_;

    absl::CondVar outcoming_call_cond_;
    uint32_t outcoming_call_handle_;
    bool outcoming_call_success_;
    std::span<const char> outcoming_call_output_;

    friend class FuncWorker;

    void AppendOutput(std::span<const char> data);
    bool InvokeFunc(std::string_view func_name, std::span<const char> input,
                    std::span<const char>* output);

    DISALLOW_COPY_AND_ASSIGN(WorkerThread);
};

FuncWorker::WorkerThread::WorkerThread(FuncWorker* func_worker, int id)
    : log_header_(absl::StrFormat("WorkerThread[%d]: ", id)), func_worker_(func_worker),
      thread_(base::Thread(absl::StrFormat("Worker-%d", id),
                           absl::bind_front(&FuncWorker::WorkerThread::ThreadMain, this))),
      outcoming_call_handle_(worker_lib::Manager::kInvalidHandle) {
    thread_.Start();
}

void FuncWorker::WorkerThread::ThreadMain() {
    base::Thread::current()->MarkThreadCategory("WORKER");

    void* worker_handle;
    int ret = func_worker_->create_func_worker_fn_(
        this, &FuncWorker::InvokeFuncWrapper,
        &FuncWorker::AppendOutputWrapper, &worker_handle);
    if (ret != 0) {
        HLOG(FATAL) << "Failed to create function worker";
    }

    func_worker_->mu_.Lock();
    while (true) {
        while (func_worker_->pending_incoming_calls_.empty()) {
            func_worker_->idle_worker_count_++;
            func_worker_->new_incoming_call_cond_.Wait(&func_worker_->mu_);
            func_worker_->idle_worker_count_--;
        }
        DCHECK(!func_worker_->pending_incoming_calls_.empty());
        FuncWorker::IncomingFuncCall call = func_worker_->pending_incoming_calls_.front();
        func_worker_->pending_incoming_calls_.pop();
        func_worker_->mu_.Unlock();

        int ret = func_worker_->func_call_fn_(
            worker_handle, call.input.data(), call.input.size());

        func_worker_->mu_.Lock();
        if (outcoming_call_handle_ != worker_lib::Manager::kInvalidHandle) {
            func_worker_->manager_.ReclaimOutcomingFuncCallOutput(outcoming_call_handle_);
            outcoming_call_handle_ = worker_lib::Manager::kInvalidHandle;
        }
        func_worker_->manager_.OnIncomingFuncCallComplete(
            call.handle, ret == 0, output_buffer_.to_span());
        output_buffer_.Reset();
    }
}

void FuncWorker::WorkerThread::AppendOutput(std::span<const char> data) {
    CHECK(base::Thread::current() == &thread_);
    output_buffer_.AppendData(data);
}

bool FuncWorker::WorkerThread::InvokeFunc(std::string_view func_name,
                                          std::span<const char> input,
                                          std::span<const char>* output) {
    CHECK(base::Thread::current() == &thread_);
    {
        uint32_t handle;
        absl::MutexLock lk(&func_worker_->mu_);
        if (outcoming_call_handle_ != worker_lib::Manager::kInvalidHandle) {
            func_worker_->manager_.ReclaimOutcomingFuncCallOutput(outcoming_call_handle_);
            outcoming_call_handle_ = worker_lib::Manager::kInvalidHandle;
        }
        if (!func_worker_->manager_.OnOutcomingFuncCall(func_name, input, &handle)) {
            return false;
        }
        func_worker_->outcoming_func_calls_[handle] = this;
        outcoming_call_handle_ = handle;
        outcoming_call_success_ = false;
        outcoming_call_cond_.Wait(&func_worker_->mu_);
    }
    if (outcoming_call_success_) {
        *output = outcoming_call_output_;
    }
    return outcoming_call_success_;
}

void FuncWorker::SignalNewDataToSend() {
    UV_DCHECK_OK(uv_async_send(&new_data_to_send_event_));
}

void FuncWorker::SendData(uv_pipe_t* uv_pipe, std::span<const char> data) {
    DCHECK_IN_EVENT_LOOP_THREAD(&uv_loop_);
    const char* ptr = data.data();
    size_t write_size = data.size();
    while (write_size > 0) {
        uv_buf_t buf;
        buffer_pool_.Get(&buf);
        size_t copy_size = std::min(buf.len, write_size);
        memcpy(buf.base, ptr, copy_size);
        buf.len = copy_size;
        uv_write_t* write_req = write_req_pool_.Get();
        write_req->data = buf.base;
        UV_DCHECK_OK(uv_write(write_req, UV_AS_STREAM(uv_pipe),
                              &buf, 1, &FuncWorker::DataSentCallback));
        write_size -= copy_size;
        ptr += copy_size;
    }
}

void FuncWorker::OnIncomingFuncCall(uint32_t handle, std::span<const char> input) {
    mu_.AssertHeld();
    pending_incoming_calls_.push({ .handle = handle, .input = input });
    if (idle_worker_count_ > 0) {
        new_incoming_call_cond_.Signal();
    }
}

void FuncWorker::OnOutcomingFuncCallComplete(uint32_t handle, bool success,
                                             std::span<const char> output,
                                             bool* reclaim_output_later) {
    mu_.AssertHeld();
    DCHECK(outcoming_func_calls_.contains(handle));
    WorkerThread* worker_thread = outcoming_func_calls_[handle];
    outcoming_func_calls_.erase(handle);
    DCHECK_EQ(worker_thread->outcoming_call_handle_, handle);
    worker_thread->outcoming_call_success_ = success;
    worker_thread->outcoming_call_output_ = output;
    worker_thread->outcoming_call_cond_.Signal();
    *reclaim_output_later = true;
}

void FuncWorker::AppendOutputWrapper(void* caller_context, const char* data, size_t length) {
    WorkerThread* worker_thread = reinterpret_cast<WorkerThread*>(caller_context);
    worker_thread->AppendOutput(std::span<const char>(data, length));
}

int FuncWorker::InvokeFuncWrapper(void* caller_context, const char* func_name,
                                  const char* input_data, size_t input_length,
                                  const char** output_data, size_t* output_length) {
    WorkerThread* worker_thread = reinterpret_cast<WorkerThread*>(caller_context);
    std::span<const char> input(input_data, input_length);
    std::span<const char> output;
    if (worker_thread->InvokeFunc(func_name, input, &output)) {
        *output_data = output.data();
        *output_length = output.size();
        return 0;
    } else {
        return -1;
    }
}

}  // namespace worker_v2
}  // namespace faas
