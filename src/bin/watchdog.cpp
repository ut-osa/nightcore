#include "base/init.h"
#include "base/common.h"

#include "watchdog/watchdog.h"

#include <absl/flags/flag.h>

ABSL_FLAG(std::string, gateway_ipc_path, "/tmp/faas_gateway",
          "Domain socket path for IPC with the gateway process");
ABSL_FLAG(std::string, function_name, "", "Function name");
ABSL_FLAG(std::string, fprocess, "", "Function process");

int main(int argc, char* argv[]) {
    faas::base::InitMain(argc, argv);

    auto watchdog = absl::make_unique<faas::watchdog::Watchdog>();
    watchdog->set_gateway_ipc_path(absl::GetFlag(FLAGS_gateway_ipc_path));
    watchdog->set_function_name(absl::GetFlag(FLAGS_function_name));
    watchdog->set_fprocess(absl::GetFlag(FLAGS_fprocess));

    watchdog->Start();
    watchdog->WaitForFinish();

    return 0;
}
