#include "base/init.h"
#include "base/common.h"
#include "utils/env_variables.h"
#include "worker/v2/func_worker.h"

int main(int argc, char* argv[]) {
    std::vector<char*> positional_args;
    faas::base::InitMain(argc, argv, &positional_args);
    if (positional_args.size() != 1) {
        LOG(FATAL) << "The only positional argument should be path to the function library";
    }

    auto func_worker = std::make_unique<faas::worker_v2::FuncWorker>();
    func_worker->set_func_library_path(positional_args[0]);
    func_worker->set_num_worker_threads(
        faas::utils::GetEnvVariableAsInt("NUM_WORKER_THREADS", 1));

    func_worker->Serve();

    return 0;
}
