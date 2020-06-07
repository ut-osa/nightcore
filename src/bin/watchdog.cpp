#include "base/init.h"
#include "base/common.h"
#include "ipc/base.h"
#include "watchdog/watchdog.h"

#include <absl/flags/flag.h>

ABSL_FLAG(std::string, root_path_for_ipc, "/dev/shm/faas_ipc",
          "Root directory for IPCs used by FaaS");
ABSL_FLAG(int, func_id, -1, "Function ID of this watchdog process");
ABSL_FLAG(std::string, fprocess, "", "Function process");
ABSL_FLAG(std::string, fprocess_working_dir, "",
          "Working directory of function processes");
ABSL_FLAG(int, run_mode, 1, "Function run mode");
ABSL_FLAG(int, min_num_func_workers, 1, "Minimum number of function workers");
ABSL_FLAG(int, max_num_func_workers, 1, "Maximum number of function workers");
ABSL_FLAG(std::string, func_worker_output_dir, "",
          "If not empty, stdout and stderr of function workers will be saved "
          "in the given directory");

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
    faas::ipc::SetRootPathForIpc(absl::GetFlag(FLAGS_root_path_for_ipc));

    auto watchdog = std::make_unique<faas::watchdog::Watchdog>();
    watchdog->set_func_id(absl::GetFlag(FLAGS_func_id));
    watchdog->set_fprocess(absl::GetFlag(FLAGS_fprocess));
    watchdog->set_fprocess_working_dir(absl::GetFlag(FLAGS_fprocess_working_dir));
    watchdog->set_run_mode(absl::GetFlag(FLAGS_run_mode));
    watchdog->set_min_num_func_workers(absl::GetFlag(FLAGS_min_num_func_workers));
    watchdog->set_max_num_func_workers(absl::GetFlag(FLAGS_max_num_func_workers));
    watchdog->set_func_worker_output_dir(absl::GetFlag(FLAGS_func_worker_output_dir));

    watchdog->Start();
    watchdog_ptr.store(watchdog.get());
    watchdog->WaitForFinish();

    return 0;
}
