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
      idle_worker_count_(0),
      shared_memory_(manager_.GetSharedMemory()) {
    if (manager_.is_grpc_service()) {
        HLOG(FATAL) << "func_worker_v2 has not implemented gRPC support";
    }
    manager_.SetIncomingFuncCallRawCallback(
        absl::bind_front(&FuncWorker::OnIncomingFuncCall, this));
    manager_.SetOutcomingFuncCallCompleteRawCallback(
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
            gateway_send_buffer_.AppendData(data);
        });
        manager_.SetSendWatchdogDataCallback([this] (std::span<const char> data) {
            mu_.AssertHeld();
            watchdog_send_buffer_.AppendData(data);
        });
        manager_.Start(/* raw_mode= */ true);
    }
    SendDataIfNecessary();

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
    utils::SharedMemory* shared_memory_;
    base::Thread thread_;
    utils::AppendableBuffer output_buffer_;

    absl::Notification outcoming_call_finished_;
    uint64_t outcoming_call_id_;
    bool outcoming_call_success_;
    utils::SharedMemory::Region* outcoming_call_output_;

    friend class FuncWorker;

    void AppendOutput(std::span<const char> data);
    bool InvokeFunc(std::string_view func_name, std::span<const char> input,
                    std::span<const char>* output);

    DISALLOW_COPY_AND_ASSIGN(WorkerThread);
};

FuncWorker::WorkerThread::WorkerThread(FuncWorker* func_worker, int id)
    : log_header_(absl::StrFormat("WorkerThread[%d]: ", id)),
      func_worker_(func_worker), shared_memory_(func_worker->shared_memory_),
      thread_(base::Thread(absl::StrFormat("Worker-%d", id),
                           absl::bind_front(&FuncWorker::WorkerThread::ThreadMain, this))),
      outcoming_call_output_(nullptr) {
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

    while (true) {
        uint64_t full_call_id;
        {
            absl::MutexLock lk(&func_worker_->mu_);
            while (func_worker_->pending_incoming_calls_.empty()) {
                func_worker_->idle_worker_count_++;
                func_worker_->new_incoming_call_cond_.Wait(&func_worker_->mu_);
                func_worker_->idle_worker_count_--;
            }
            DCHECK(!func_worker_->pending_incoming_calls_.empty());
            full_call_id = func_worker_->pending_incoming_calls_.front();
            func_worker_->pending_incoming_calls_.pop();
            func_worker_->mu_.Unlock();
        }

        utils::SharedMemory::Region* input_region = shared_memory_->OpenReadOnly(
            utils::SharedMemory::InputPath(full_call_id));
        int ret = func_worker_->func_call_fn_(
            worker_handle, input_region->base(), input_region->size());
        input_region->Close();
        if (ret == 0) {
            utils::SharedMemory::Region* output_region = shared_memory_->Create(
                utils::SharedMemory::OutputPath(full_call_id), output_buffer_.length());
            if (output_buffer_.length() > 0) {
                memcpy(output_region->base(), output_buffer_.data(), output_buffer_.length());
            }
            output_region->Close();
        }
        output_buffer_.Reset();
        if (outcoming_call_output_ != nullptr) {
            outcoming_call_output_->Close(true);
            outcoming_call_output_ = nullptr;
        }

        {
            absl::MutexLock lk(&func_worker_->mu_);
            func_worker_->manager_.OnIncomingFuncCallCompleteRaw(full_call_id, ret == 0);
        }
        func_worker_->SendDataIfNecessary();
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
    if (outcoming_call_output_ != nullptr) {
        outcoming_call_output_->Close(true);
        outcoming_call_output_ = nullptr;
    }
    uint64_t full_call_id;
    {
        absl::MutexLock lk(&func_worker_->mu_);
        if (!func_worker_->manager_.OnOutcomingFuncCallRaw(func_name, &full_call_id)) {
            return false;
        }
    }
    outcoming_call_id_ = full_call_id;
    outcoming_call_success_ = false;
    utils::SharedMemory::Region* input_region = shared_memory_->Create(
        utils::SharedMemory::InputPath(full_call_id), input.size());
    if (input.size() > 0) {
        memcpy(input_region->base(), input.data(), input.size());
    }
    {
        absl::MutexLock lk(&func_worker_->mu_);
        func_worker_->outcoming_func_calls_[full_call_id] = this;
        func_worker_->manager_.OnSendOutcomingFuncCall(full_call_id);
    }
    new(&outcoming_call_finished_) absl::Notification;
    func_worker_->SendDataIfNecessary();
    outcoming_call_finished_.WaitForNotification();
    if (outcoming_call_success_) {
        outcoming_call_output_ = shared_memory_->OpenReadOnly(
            utils::SharedMemory::OutputPath(full_call_id));
        *output = outcoming_call_output_->to_span();
    }
    return outcoming_call_success_;
}

void FuncWorker::OnIncomingFuncCall(uint64_t full_call_id) {
    mu_.AssertHeld();
    pending_incoming_calls_.push(full_call_id);
    if (idle_worker_count_ > 0) {
        new_incoming_call_cond_.Signal();
    }
}

void FuncWorker::OnOutcomingFuncCallComplete(uint64_t full_call_id, bool success) {
    mu_.AssertHeld();
    DCHECK(outcoming_func_calls_.contains(full_call_id));
    WorkerThread* worker_thread = outcoming_func_calls_[full_call_id];
    outcoming_func_calls_.erase(full_call_id);
    DCHECK_EQ(worker_thread->outcoming_call_id_, full_call_id);
    worker_thread->outcoming_call_success_ = success;
    worker_thread->outcoming_call_finished_.Notify();
}

void FuncWorker::SendDataIfNecessary() {
    mu_.AssertNotHeld();
    absl::MutexLock lk(&write_mu_);
    write_buffer_.Reset();
    {
        absl::MutexLock lk(&mu_);
        if (gateway_send_buffer_.length() > 0) {
            write_buffer_.Swap(gateway_send_buffer_);
        }
    }
    if (write_buffer_.length() > 0) {
        io_utils::SendData(gateway_ipc_socket_, write_buffer_.to_span());
        write_buffer_.Reset();
    }
    {
        absl::MutexLock lk(&mu_);
        if (watchdog_send_buffer_.length() > 0) {
            write_buffer_.Swap(watchdog_send_buffer_);
        }
    }
    if (write_buffer_.length() > 0) {
        io_utils::SendData(watchdog_output_pipe_fd_, write_buffer_.to_span());
        write_buffer_.Reset();
    }
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
