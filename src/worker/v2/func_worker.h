#pragma once

#ifndef __FAAS_SRC
#error worker/v2/func_worker.h cannot be included outside
#endif

#include "base/common.h"
#include "common/uv.h"
#include "utils/dynamic_library.h"
#include "utils/appendable_buffer.h"
#include "utils/buffer_pool.h"
#include "utils/object_pool.h"
#include "worker/lib/manager.h"
#include "faas/worker_v1_interface.h"

namespace faas {
namespace worker_v2 {

class FuncWorker : public uv::Base {
public:
    static constexpr int kDefaultWorkerThreadNumber = 1;
    static constexpr size_t kBufferSize = 64;

    FuncWorker();
    ~FuncWorker();

    void set_func_library_path(std::string_view path) {
        func_library_path_ = std::string(path);
    }
    void set_num_worker_threads(int value) {
        num_worker_threads_ = value;
    }

    void Serve();

private:
    std::string log_header_;
    std::string func_library_path_;
    int num_worker_threads_;

    uv_loop_t uv_loop_;
    uv_connect_t gateway_connect_req_;
    uv_pipe_t gateway_ipc_handle_;
    uv_pipe_t watchdog_input_pipe_handle_;
    uv_pipe_t watchdog_output_pipe_handle_;
    uv_async_t new_data_to_send_event_;

    utils::BufferPool buffer_pool_;
    utils::SimpleObjectPool<uv_write_t> write_req_pool_;

    class WorkerThread;
    std::vector<std::unique_ptr<WorkerThread>> worker_threads_;

    std::unique_ptr<utils::DynamicLibrary> func_library_;
    faas_init_fn_t init_fn_;
    faas_create_func_worker_fn_t create_func_worker_fn_;
    faas_destroy_func_worker_fn_t destroy_func_worker_fn_;
    faas_func_call_fn_t func_call_fn_;

    absl::Mutex mu_;
    absl::CondVar new_incoming_call_cond_;
    int idle_worker_count_ ABSL_GUARDED_BY(mu_);

    worker_lib::Manager manager_ ABSL_GUARDED_BY(mu_);
    absl::Mutex gateway_send_buffer_mu_;
    utils::AppendableBuffer gateway_send_buffer_ ABSL_GUARDED_BY(gateway_send_buffer_mu_);
    absl::Mutex watchdog_send_buffer_mu_;
    utils::AppendableBuffer watchdog_send_buffer_ ABSL_GUARDED_BY(watchdog_send_buffer_mu_);

    struct IncomingFuncCall {
        uint32_t handle;
        std::span<const char> input;
    };
    std::queue<IncomingFuncCall> pending_incoming_calls_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map<uint32_t, WorkerThread*> outcoming_func_calls_ ABSL_GUARDED_BY(mu_);

    friend class WorkerThread;

    void SignalNewDataToSend();
    void SendData(uv_pipe_t* uv_pipe, std::span<const char> data);
    void OnIncomingFuncCall(uint32_t handle, std::span<const char> input);
    void OnOutcomingFuncCallComplete(uint32_t handle, bool success, std::span<const char> output,
                                     bool* reclaim_output_later);

    // Assume caller_context is an instance of WorkerThread
    static void AppendOutputWrapper(void* caller_context, const char* data, size_t length);
    static int InvokeFuncWrapper(void* caller_context, const char* func_name,
                                 const char* input_data, size_t input_length,
                                 const char** output_data, size_t* output_length);

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_CONNECT_CB_FOR_CLASS(GatewayConnect);
    DECLARE_UV_READ_CB_FOR_CLASS(RecvGatewayData);
    DECLARE_UV_READ_CB_FOR_CLASS(RecvWatchdogData);
    DECLARE_UV_ASYNC_CB_FOR_CLASS(NewDataToSend);
    DECLARE_UV_WRITE_CB_FOR_CLASS(DataSent);

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace worker_v2
}  // namespace faas
