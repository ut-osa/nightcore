#pragma once

#include "base/common.h"
#include "utils/shared_memory.h"
#include "utils/dynamic_library.h"
#include "utils/appendable_buffer.h"
#include "faas/sync_interface.h"

namespace faas {
namespace worker_v1 {

class FuncWorker {
public:
    FuncWorker();
    ~FuncWorker();

    void set_gateway_ipc_path(absl::string_view gateway_ipc_path) {
        gateway_ipc_path_ = std::string(gateway_ipc_path);
    }
    void set_func_id(int func_id) {
        func_id_ = func_id;
    }
    void set_func_library_path(absl::string_view func_library_path) {
        func_library_path_ = std::string(func_library_path);
    }
    void set_input_pipe_fd(int input_pipe_fd) {
        input_pipe_fd_ = input_pipe_fd;
    }
    void set_output_pipe_fd(int output_pipe_fd) {
        output_pipe_fd_ = output_pipe_fd;
    }
    void set_shared_mem_path(absl::string_view shared_mem_path) {
        shared_mem_path_ = std::string(shared_mem_path);
    }

    void Serve();

private:
    std::string gateway_ipc_path_;
    int func_id_;
    std::string func_library_path_;
    int input_pipe_fd_;
    int output_pipe_fd_;
    std::string shared_mem_path_;

    int gateway_sock_fd_;
    uint16_t client_id_;
    std::atomic<bool> gateway_disconnected_;
    base::Thread gateway_ipc_thread_;

    std::unique_ptr<utils::SharedMemory> shared_memory_;
    std::unique_ptr<utils::DynamicLibrary> func_library_;

    faas_init_fn_t init_fn_;
    faas_create_func_worker_fn_t create_func_worker_fn_;
    faas_destroy_func_worker_fn_t destroy_func_worker_fn_;
    faas_func_call_fn_t func_call_fn_;

    utils::AppendableBuffer func_output_buffer_;

    void MainServingLoop();
    void GatewayIpcHandshake();
    void GatewayIpcThreadMain();

    bool InvokeFunc(void* worker_handle, uint64_t call_id);
    // Assume caller_context is an instance of FuncWorker
    static void AppendOutputWrapper(void* caller_context, const char* data, size_t length);

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace worker_v1
}  // namespace faas
