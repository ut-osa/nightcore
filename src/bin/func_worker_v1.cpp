#include "base/init.h"
#include "base/common.h"
#include "utils/env_variables.h"
#include "worker/v1/func_worker.h"

int main(int argc, char* argv[]) {
    std::vector<char*> positional_args;
    faas::base::InitMain(argc, argv, &positional_args);
    if (positional_args.size() != 1) {
        LOG(FATAL) << "The only positional argument should be path to the function library";
    }

    auto func_worker = std::make_unique<faas::worker_v1::FuncWorker>();
    func_worker->set_gateway_ipc_path(
        faas::utils::GetEnvVariable("GATEWAY_IPC_PATH", "/tmp/faas_gateway"));
    func_worker->set_func_id(
        faas::utils::GetEnvVariableAsInt("FUNC_ID", -1));
    func_worker->set_func_library_path(positional_args[0]);
    func_worker->set_input_pipe_fd(
        faas::utils::GetEnvVariableAsInt("INPUT_PIPE_FD", -1));
    func_worker->set_output_pipe_fd(
        faas::utils::GetEnvVariableAsInt("OUTPUT_PIPE_FD", -1));
    func_worker->set_shared_mem_path(
        faas::utils::GetEnvVariable("SHARED_MEMORY_PATH", "/dev/shm/faas"));
    func_worker->set_func_config_file(
        faas::utils::GetEnvVariable("FUNC_CONFIG_FILE"));

    func_worker->Serve();

    return 0;
}
