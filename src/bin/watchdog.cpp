#include "base/init.h"
#include "base/common.h"

#include "watchdog/watchdog.h"

#include <absl/flags/flag.h>

ABSL_FLAG(std::string, gateway_ipc_path, "/tmp/faas_gateway",
          "Domain socket path for IPC with the gateway process");
ABSL_FLAG(std::string, shared_mem_path, "/dev/shm/faas",
          "Root directory for shared memories used by FaaS");
ABSL_FLAG(std::string, func_config_file, "", "Path to function config file");
ABSL_FLAG(int, func_id, -1, "Function ID of this watchdog process");
ABSL_FLAG(std::string, fprocess, "", "Function process");
ABSL_FLAG(int, run_mode, 1, "Function run mode");
ABSL_FLAG(int, min_num_func_workers, 1, "Minimum number of function workers");
ABSL_FLAG(int, max_num_func_workers, 2, "Maximum number of function workers");
ABSL_FLAG(std::string, func_worker_output_dir, "",
          "If not empty, stdout and stderr of function workers will be saved "
          "in the given directory");
ABSL_FLAG(int, go_max_procs, 1, "Value of GOMAXPROCS for Golang function workers");

static std::atomic<faas::watchdog::Watchdog*> watchdog_ptr(nullptr);
void SignalHandlerToStopWatchdog(int signal) {
    faas::watchdog::Watchdog* watchdog = watchdog_ptr.exchange(nullptr);
    if (watchdog != nullptr) {
        watchdog->ScheduleStop();
    }
}

int main(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopWatchdog);
    faas::base::InitMain(argc, argv);

    auto watchdog = std::make_unique<faas::watchdog::Watchdog>();
    watchdog->set_gateway_ipc_path(absl::GetFlag(FLAGS_gateway_ipc_path));
    watchdog->set_func_id(absl::GetFlag(FLAGS_func_id));
    watchdog->set_fprocess(absl::GetFlag(FLAGS_fprocess));
    watchdog->set_shared_mem_path(absl::GetFlag(FLAGS_shared_mem_path));
    watchdog->set_run_mode(absl::GetFlag(FLAGS_run_mode));
    watchdog->set_min_num_func_workers(absl::GetFlag(FLAGS_min_num_func_workers));
    watchdog->set_max_num_func_workers(absl::GetFlag(FLAGS_max_num_func_workers));
    watchdog->set_func_config_file(absl::GetFlag(FLAGS_func_config_file));
    watchdog->set_func_worker_output_dir(absl::GetFlag(FLAGS_func_worker_output_dir));
    watchdog->set_go_max_procs(absl::GetFlag(FLAGS_go_max_procs));

    watchdog->Start();
    watchdog_ptr.store(watchdog.get());
    watchdog->WaitForFinish();

    return 0;
}
