#include "base/init.h"
#include "base/common.h"
#include "ipc/base.h"
#include "launcher/launcher.h"

#include <absl/flags/flag.h>

ABSL_FLAG(std::string, root_path_for_ipc, "/dev/shm/faas_ipc",
          "Root directory for IPCs used by FaaS");
ABSL_FLAG(int, func_id, -1, "Function ID of this launcher process");
ABSL_FLAG(std::string, fprocess, "", "Function process");
ABSL_FLAG(std::string, fprocess_working_dir, "",
          "Working directory of function processes");
ABSL_FLAG(std::string, fprocess_output_dir, "",
          "If not empty, stdout and stderr of function processes will be saved "
          "in the given directory");
ABSL_FLAG(bool, fprocess_multi_worker_mode, false,
          "If true, one function process can act as multiple function workers. "
          "In this case, the launcher will create message pipes to function processes.");

static std::atomic<faas::launcher::Launcher*> launcher_ptr(nullptr);
void SignalHandlerToStopLauncher(int signal) {
    faas::launcher::Launcher* launcher = launcher_ptr.exchange(nullptr);
    if (launcher != nullptr) {
        launcher->ScheduleStop();
    }
}

int main(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopLauncher);
    faas::base::InitMain(argc, argv);
    faas::ipc::SetRootPathForIpc(absl::GetFlag(FLAGS_root_path_for_ipc));

    auto launcher = std::make_unique<faas::launcher::Launcher>();
    launcher->set_func_id(absl::GetFlag(FLAGS_func_id));
    launcher->set_fprocess(absl::GetFlag(FLAGS_fprocess));
    launcher->set_fprocess_working_dir(absl::GetFlag(FLAGS_fprocess_working_dir));
    launcher->set_fprocess_output_dir(absl::GetFlag(FLAGS_fprocess_output_dir));
    launcher->set_fprocess_multi_worker_mode(absl::GetFlag(FLAGS_fprocess_multi_worker_mode));

    launcher->Start();
    launcher_ptr.store(launcher.get());
    launcher->WaitForFinish();

    return 0;
}
