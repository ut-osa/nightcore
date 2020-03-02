#include "base/init.h"
#include "base/common.h"
#include "gateway/server.h"

#include <signal.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, listen_addr, "0.0.0.0", "Address to listen");
ABSL_FLAG(int, listen_port, 8080, "Port to listen");
ABSL_FLAG(int, num_io_workers, 2, "Number of IO workers");

static faas::gateway::Server* server_ptr = nullptr;
void SignalHandlerToCloseServer(int signal) {
    if (server_ptr != nullptr) {
        server_ptr->ScheduleStop();
    }
}

int main(int argc, char* argv[]) {
    faas::base::InitMain(argc, argv);

    auto server = absl::make_unique<faas::gateway::Server>();
    server->set_address(absl::GetFlag(FLAGS_listen_addr));
    server->set_port(absl::GetFlag(FLAGS_listen_port));
    server->set_num_io_workers(absl::GetFlag(FLAGS_num_io_workers));

    server->Start();
    server_ptr = server.get();
    signal(SIGINT, SignalHandlerToCloseServer);
    signal(SIGTERM, SignalHandlerToCloseServer);

    server->WaitForFinish();

    return 0;
}
