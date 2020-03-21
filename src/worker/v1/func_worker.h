#pragma once

#include "base/common.h"
#include "common/stat.h"
#include "common/func_config.h"
#include "utils/shared_memory.h"
#include "utils/dynamic_library.h"
#include "utils/appendable_buffer.h"
#include "faas/worker_v1_interface.h"

namespace faas {
namespace worker_v1 {

class FuncWorker {
public:
    FuncWorker();
    ~FuncWorker();

    void set_gateway_ipc_path(absl::string_view path) {
        gateway_ipc_path_ = std::string(path);
    }
    void set_func_id(int func_id) {
        func_id_ = func_id;
    }
    void set_func_library_path(absl::string_view path) {
        func_library_path_ = std::string(path);
    }
    void set_input_pipe_fd(int input_pipe_fd) {
        input_pipe_fd_ = input_pipe_fd;
    }
    void set_output_pipe_fd(int output_pipe_fd) {
        output_pipe_fd_ = output_pipe_fd;
    }
    void set_shared_mem_path(absl::string_view path) {
        shared_mem_path_ = std::string(path);
    }
    void set_func_config_file(absl::string_view path) {
        func_config_file_ = std::string(path);
    }

    void Serve();

private:
    std::string gateway_ipc_path_;
    int func_id_;
    std::string func_library_path_;
    int input_pipe_fd_;
    int output_pipe_fd_;
    std::string shared_mem_path_;
    std::string func_config_file_;

    int gateway_sock_fd_;
    uint16_t client_id_;
    std::atomic<bool> gateway_disconnected_;
    base::Thread gateway_ipc_thread_;

    FuncConfig func_config_;
    std::unique_ptr<utils::SharedMemory> shared_memory_;
    std::unique_ptr<utils::DynamicLibrary> func_library_;

    faas_init_fn_t init_fn_;
    faas_create_func_worker_fn_t create_func_worker_fn_;
    faas_destroy_func_worker_fn_t destroy_func_worker_fn_;
    faas_func_call_fn_t func_call_fn_;

    utils::AppendableBuffer func_output_buffer_;

    struct FuncInvokeContext {
        absl::Notification finished;
        bool success;
        uint32_t processing_time;
        utils::SharedMemory::Region* input_region;
        utils::SharedMemory::Region* output_region;
    };

    absl::Mutex invoke_func_mu_;
    std::atomic<uint32_t> next_call_id_;
    absl::flat_hash_map<uint64_t, std::unique_ptr<FuncInvokeContext>>
        func_invoke_contexts_ ABSL_GUARDED_BY(invoke_func_mu_);

    stat::StatisticsCollector<uint32_t> gateway_message_delay_stat_;
    stat::StatisticsCollector<uint32_t> watchdog_message_delay_stat_;
    stat::StatisticsCollector<uint32_t> processing_delay_stat_;
    stat::StatisticsCollector<uint32_t> system_protocol_overhead_stat_;

    void MainServingLoop();
    void GatewayIpcHandshake();
    void GatewayIpcThreadMain();

    bool RunFuncHandler(void* worker_handle, uint64_t call_id);
    bool InvokeFunc(const char* func_name,
                    const char* input_data, size_t input_length,
                    const char** output_data, size_t* output_length);

    // Assume caller_context is an instance of FuncWorker
    static void AppendOutputWrapper(void* caller_context, const char* data, size_t length);
    static int InvokeFuncWrapper(void* caller_context, const char* func_name,
                                 const char* input_data, size_t input_length,
                                 const char** output_data, size_t* output_length);

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace worker_v1
}  // namespace faas
