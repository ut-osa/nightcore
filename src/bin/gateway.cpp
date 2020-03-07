#include "base/init.h"
#include "base/common.h"
#include "gateway/server.h"

#include <signal.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, func_config_file, "", "Path to function config file");
ABSL_FLAG(std::string, listen_addr, "0.0.0.0", "Address to listen");
ABSL_FLAG(int, listen_port, 8080, "Port to listen");
ABSL_FLAG(int, num_http_workers, 1, "Number of HTTP workers");
ABSL_FLAG(int, num_ipc_workers, 1, "Number of IPC workers");
ABSL_FLAG(std::string, ipc_path, "/tmp/faas_gateway",
          "Domain socket path for IPC with watchdog processes");

static std::atomic<faas::gateway::Server*> server_ptr(nullptr);
void SignalHandlerToCloseServer(int signal) {
    faas::gateway::Server* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

int main(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToCloseServer);
    faas::base::InitMain(argc, argv);

    auto server = absl::make_unique<faas::gateway::Server>();
    server->set_address(absl::GetFlag(FLAGS_listen_addr));
    server->set_ipc_path(absl::GetFlag(FLAGS_ipc_path));
    server->set_port(absl::GetFlag(FLAGS_listen_port));
    server->set_num_http_workers(absl::GetFlag(FLAGS_num_http_workers));
    server->set_num_ipc_workers(absl::GetFlag(FLAGS_num_ipc_workers));

    server->Start();
    server_ptr.store(server.get());
    server->WaitForFinish();

    return 0;
}
