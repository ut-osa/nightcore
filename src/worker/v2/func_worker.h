#pragma once

#ifndef __FAAS_SRC
#error worker/v2/func_worker.h cannot be included outside
#endif

#include "base/common.h"
#include "utils/dynamic_library.h"
#include "utils/appendable_buffer.h"
#include "worker/lib/manager.h"
#include "faas/worker_v1_interface.h"

namespace faas {
namespace worker_v2 {

class FuncWorker {
public:
    static constexpr int kDefaultWorkerThreadNumber = 1;
    static constexpr size_t kBufferSize = 256;

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
    int gateway_ipc_socket_;
    int watchdog_input_pipe_fd_;
    int watchdog_output_pipe_fd_;

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

    struct IncomingFuncCall {
        uint32_t handle;
        std::span<const char> input;
    };
    std::queue<IncomingFuncCall> pending_incoming_calls_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map<uint32_t, WorkerThread*> outcoming_func_calls_ ABSL_GUARDED_BY(mu_);

    friend class WorkerThread;

    void OnIncomingFuncCall(uint32_t handle, std::span<const char> input);
    void OnOutcomingFuncCallComplete(uint32_t handle, bool success, std::span<const char> output,
                                     bool* reclaim_output_later);

    // Assume caller_context is an instance of WorkerThread
    static void AppendOutputWrapper(void* caller_context, const char* data, size_t length);
    static int InvokeFuncWrapper(void* caller_context, const char* func_name,
                                 const char* input_data, size_t input_length,
                                 const char** output_data, size_t* output_length);

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace worker_v2
}  // namespace faas
