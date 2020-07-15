#include "base/init.h"
#include "base/common.h"
#include "ipc/base.h"
#include "utils/env_variables.h"
#include "worker/v1/func_worker.h"

int main(int argc, char* argv[]) {
    std::vector<char*> positional_args;
    faas::base::InitMain(argc, argv, &positional_args);
    if (positional_args.size() != 1) {
        LOG(FATAL) << "The only positional argument should be path to the function library";
    }
    faas::ipc::SetRootPathForIpc(
        faas::utils::GetEnvVariable("FAAS_ROOT_PATH_FOR_IPC", "/dev/shm/faas_ipc"));

    auto func_worker = std::make_unique<faas::worker_v1::FuncWorker>();
    func_worker->set_func_id(
        faas::utils::GetEnvVariableAsInt("FAAS_FUNC_ID", -1));
    func_worker->set_fprocess_id(
        faas::utils::GetEnvVariableAsInt("FAAS_FPROCESS_ID", -1));
    func_worker->set_client_id(
        faas::utils::GetEnvVariableAsInt("FAAS_CLIENT_ID", 0));
    func_worker->set_message_pipe_fd(
        faas::utils::GetEnvVariableAsInt("FAAS_MSG_PIPE_FD", -1));
    if (faas::utils::GetEnvVariableAsInt("FAAS_USE_ENGINE_SOCKET", 0) == 1) {
        func_worker->enable_use_engine_socket();
    }
    func_worker->set_engine_tcp_port(
        faas::utils::GetEnvVariableAsInt("FAAS_ENGINE_TCP_PORT", -1));
    func_worker->set_func_library_path(positional_args[0]);
    func_worker->Serve();

    return 0;
}
