#pragma once

#ifndef __FAAS_SRC
#error worker/v1/func_worker.h cannot be included outside
#endif

#include "base/common.h"
#include "base/thread.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "utils/dynamic_library.h"
#include "utils/appendable_buffer.h"
#include "utils/object_pool.h"
#include "faas/worker_v1_interface.h"

namespace faas {
namespace worker_v1 {

class FuncWorker {
public:
    FuncWorker();
    ~FuncWorker();

    void set_func_id(int value) { func_id_ = value; }
    void set_fprocess_id(int value) { fprocess_id_ = value; }
    void set_client_id(int value) { client_id_ = gsl::narrow_cast<uint16_t>(value); }
    void set_func_library_path(std::string_view path) {
        func_library_path_ = std::string(path);
    }

    void Serve();

private:
    int func_id_;
    int fprocess_id_;
    uint16_t client_id_;
    std::string func_library_path_;

    absl::Mutex mu_;

    int gateway_sock_fd_;
    int input_pipe_fd_;
    int output_pipe_fd_;

    FuncConfig func_config_;
    std::unique_ptr<utils::DynamicLibrary> func_library_;

    faas_init_fn_t init_fn_;
    faas_create_func_worker_fn_t create_func_worker_fn_;
    faas_destroy_func_worker_fn_t destroy_func_worker_fn_;
    faas_func_call_fn_t func_call_fn_;

    absl::flat_hash_set<char*> temporary_buffers_ ABSL_GUARDED_BY(mu_);
    utils::AppendableBuffer func_output_buffer_;

    std::atomic<uint32_t> next_call_id_;

    stat::StatisticsCollector<int32_t> gateway_message_delay_stat_;
    stat::StatisticsCollector<int32_t> processing_delay_stat_;
    stat::StatisticsCollector<uint32_t> input_size_stat_;
    stat::StatisticsCollector<uint32_t> output_size_stat_;

    void MainServingLoop();
    void HandshakeWithGateway();

    void ExecuteFunc(void* worker_handle, const protocol::FuncCall& func_call);
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
