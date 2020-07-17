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
#include "utils/buffer_pool.h"
#include "ipc/shm_region.h"
#include "faas/worker_v1_interface.h"

namespace faas {
namespace worker_v1 {

class FuncWorker {
public:
    static constexpr absl::Duration kDefaultFuncCallTimeout = absl::Milliseconds(100);

    FuncWorker();
    ~FuncWorker();

    void set_func_id(int value) { func_id_ = value; }
    void set_fprocess_id(int value) { fprocess_id_ = value; }
    void set_client_id(int value) { client_id_ = gsl::narrow_cast<uint16_t>(value); }
    void set_message_pipe_fd(int fd) { message_pipe_fd_ = fd; }
    void set_func_library_path(std::string_view path) {
        func_library_path_ = std::string(path);
    }
    void enable_use_engine_socket() { use_engine_socket_ = true; }
    void set_engine_tcp_port(int port) { engine_tcp_port_ = port; }

    void Serve();

private:
    int func_id_;
    int fprocess_id_;
    uint16_t client_id_;
    int message_pipe_fd_;
    std::string func_library_path_;
    bool use_engine_socket_;
    int engine_tcp_port_;
    bool use_fifo_for_nested_call_;
    absl::Duration func_call_timeout_;

    absl::Mutex mu_;

    int engine_sock_fd_;
    int input_pipe_fd_;
    int output_pipe_fd_;

    FuncConfig func_config_;
    std::unique_ptr<utils::DynamicLibrary> func_library_;
    void* worker_handle_;

    faas_init_fn_t init_fn_;
    faas_create_func_worker_fn_t create_func_worker_fn_;
    faas_destroy_func_worker_fn_t destroy_func_worker_fn_;
    faas_func_call_fn_t func_call_fn_;

    struct InvokeFuncResource {
        protocol::FuncCall func_call;
        std::unique_ptr<ipc::ShmRegion> output_region;
        char* pipe_buffer;
    };

    std::vector<InvokeFuncResource> invoke_func_resources_ ABSL_GUARDED_BY(mu_);
    utils::BufferPool buffer_pool_for_pipes_ ABSL_GUARDED_BY(mu_);
    bool ongoing_invoke_func_ ABSL_GUARDED_BY(mu_);
    utils::AppendableBuffer func_output_buffer_;
    char main_pipe_buf_[PIPE_BUF];

    std::atomic<uint32_t> next_call_id_;
    std::atomic<uint64_t> current_func_call_id_;

    void MainServingLoop();
    void HandshakeWithEngine();

    void ExecuteFunc(const protocol::Message& dispatch_func_call_message);
    bool InvokeFunc(const char* func_name,
                    const char* input_data, size_t input_length,
                    const char** output_data, size_t* output_length);
    bool WaitInvokeFunc(protocol::Message* invoke_func_message,
                        const char** output_data, size_t* output_length);
    bool FifoWaitInvokeFunc(protocol::Message* invoke_func_message,
                            const char** output_data, size_t* output_length);
    void ReclaimInvokeFuncResources();

    // Assume caller_context is an instance of FuncWorker
    static void AppendOutputWrapper(void* caller_context, const char* data, size_t length);
    static int InvokeFuncWrapper(void* caller_context, const char* func_name,
                                 const char* input_data, size_t input_length,
                                 const char** output_data, size_t* output_length);

    DISALLOW_COPY_AND_ASSIGN(FuncWorker);
};

}  // namespace worker_v1
}  // namespace faas
