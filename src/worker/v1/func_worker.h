#pragma once

#include "base/common.h"
#include "utils/shared_memory.h"
#include "utils/dynamic_library.h"
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
    void set_func_library_path(absl::string_view func_library_path) {
        func_library_path_ = std::string(func_library_path);
    }
    void set_input_pipe_fd(int value) {
        input_pipe_fd_ = value;
    }
    void set_output_pipe_fd(int value) {
        output_pipe_fd_ = value;
    }
    void set_shared_mem_path(absl::string_view shared_mem_path) {
        shared_mem_path_ = std::string(shared_mem_path_);
    }

    void Serve();

private:
    std::string gateway_ipc_path_;
    std::string func_library_path_;
    int input_pipe_fd_;
    int output_pipe_fd_;
    std::string shared_mem_path_;

    int gateway_sock_fd_;

    std::unique_ptr<utils::SharedMemory> shared_memory_;
    std::unique_ptr<utils::DynamicLibrary> func_library_;

    faas_init_fn_t init_fn_;
    faas_create_func_worker_fn_t create_func_worker_fn_;
    faas_destroy_func_worker_fn_t destroy_func_worker_fn_;
    fass_func_call_fn_t func_call_fn_;

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace worker_v1
}  // namespace faas
