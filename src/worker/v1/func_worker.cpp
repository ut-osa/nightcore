#include "worker/v1/func_worker.h"

#include "utils/socket.h"

int AddNumbers(int a, int b);

namespace faas {
namespace worker_v1 {

FuncWorker::FuncWorker()
    : input_pipe_fd_(-1), output_pipe_fd_(-1) {}

FuncWorker::~FuncWorker() {}

void FuncWorker::Serve() {
    // Load function library
    CHECK(!func_library_path_.empty());
    func_library_ = utils::DynamicLibrary::Create(func_library_path_);
    init_fn_ = func_library_->LoadSymbol<faas_init_fn_t>("faas_init");
    create_func_worker_fn_ = func_library_->LoadSymbol<faas_create_func_worker_fn_t>(
        "faas_create_func_worker");
    destroy_func_worker_fn_ = func_library_->LoadSymbol<faas_destroy_func_worker_fn_t>(
        "faas_destroy_func_worker");
    func_call_fn_ = func_library_->LoadSymbol<fass_func_call_fn_t>(
        "fass_func_call");
    CHECK(init_fn_() == 0) << "Failed to initialize loaded library";
    // Ensure we know pipe fds to the watchdog
    CHECK(input_pipe_fd_ != -1);
    CHECK(output_pipe_fd_ != -1);
    // Connect to gateway via IPC path
    CHECK(!gateway_ipc_path_.empty());
    gateway_sock_fd_ = utils::UnixDomainSocketConnect(gateway_ipc_path_);
    // Create shared memory pool
    CHECK(!shared_mem_path_.empty());
    shared_memory_ = absl::make_unique<utils::SharedMemory>(shared_mem_path_);
}

}  // namespace worker_v1
}  // namespace faas
