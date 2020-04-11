#include "worker/v2/func_worker.h"

#include "base/thread.h"
#include "utils/socket.h"
#include "utils/io.h"

#include <poll.h>

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace worker_v2 {

constexpr int FuncWorker::kDefaultWorkerThreadNumber;
constexpr size_t FuncWorker::kBufferSize;

FuncWorker::FuncWorker()
    : log_header_("FuncWorker: "),
      num_worker_threads_(kDefaultWorkerThreadNumber),
      idle_worker_count_(0) {
    if (manager_.is_grpc_service()) {
        HLOG(FATAL) << "func_worker_v2 has not implemented gRPC support";
    }
    manager_.SetIncomingFuncCallCallback(
        absl::bind_front(&FuncWorker::OnIncomingFuncCall, this));
    manager_.SetOutcomingFuncCallCompleteCallback(
        absl::bind_front(&FuncWorker::OnOutcomingFuncCallComplete, this));
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
        gateway_ipc_socket_ = utils::UnixDomainSocketConnect(manager_.gateway_ipc_path());
        watchdog_input_pipe_fd_ = manager_.watchdog_input_pipe_fd();
        watchdog_output_pipe_fd_ = manager_.watchdog_output_pipe_fd();
        manager_.SetSendGatewayDataCallback([this] (std::span<const char> data) {
            mu_.AssertHeld();
            io_utils::SendData(gateway_ipc_socket_, data);
        });
        manager_.SetSendWatchdogDataCallback([this] (std::span<const char> data) {
            mu_.AssertHeld();
            io_utils::SendData(watchdog_output_pipe_fd_, data);
        });
        manager_.Start();
    }

    CHECK_GT(num_worker_threads_, 0);
    HLOG(INFO) << "Create " << num_worker_threads_
               << " worker threads for running function handler";
    for (int i = 0; i < num_worker_threads_; i++) {
        worker_threads_.push_back(std::make_unique<WorkerThread>(this, i));
    }

    char read_buffer[kBufferSize];
    pollfd fds[2];
    fds[0].fd = gateway_ipc_socket_;
    fds[0].events = POLLIN;
    fds[1].fd = watchdog_input_pipe_fd_;
    fds[1].events = POLLIN;

    HLOG(INFO) << "Start reading from gateway and watchdog";
    base::Thread::current()->MarkThreadCategory("WORKER_IO");

    while (true) {
        int ret = poll(fds, 2, -1);
        if (ret == -1) {
            PLOG(FATAL) << "poll returns with error";
        }
        CHECK(ret > 0);
        auto errstr = [] (short revents) -> std::string {
            std::string result = "";
            if (revents & POLLNVAL) result += "POLLNVAL ";
            if (revents & POLLERR) result += "POLLERR ";
            if (revents & POLLHUP) result += "POLLHUP ";
            return std::string(absl::StripTrailingAsciiWhitespace(result));
        };
        {
            absl::MutexLock lk(&mu_);
            if ((fds[0].revents & POLLNVAL) || (fds[0].revents & POLLERR) || (fds[0].revents & POLLHUP) ) {
                manager_.OnGatewayIOError(errstr(fds[0].revents));
            }
            if ((fds[1].revents & POLLNVAL) || (fds[1].revents & POLLERR) || (fds[1].revents & POLLHUP) ) {
                manager_.OnWatchdogIOError(errstr(fds[1].revents));
            }
        }
        if (fds[0].revents & POLLIN) {
            ssize_t nread = read(gateway_ipc_socket_, read_buffer, kBufferSize);
            {
                absl::MutexLock lk(&mu_);
                if (nread < 0) {
                    manager_.OnGatewayIOError(errno);
                } else if (nread > 0) {
                    manager_.OnRecvGatewayData(std::span<const char>(read_buffer, nread));
                } else {
                    HLOG(WARNING) << "read returns with 0";
                }
            }
        }
        if (fds[1].revents & POLLIN) {
            ssize_t nread = read(watchdog_input_pipe_fd_, read_buffer, kBufferSize);
            {
                absl::MutexLock lk(&mu_);
                if (nread < 0) {
                    manager_.OnWatchdogIOError(errno);
                } else if (nread > 0) {
                    manager_.OnRecvWatchdogData(std::span<const char>(read_buffer, nread));
                } else {
                    HLOG(WARNING) << "read returns with 0";
                }
            }
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
