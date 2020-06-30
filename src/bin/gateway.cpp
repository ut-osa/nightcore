#include "base/init.h"
#include "base/common.h"
#include "ipc/base.h"
#include "utils/docker.h"
#include "utils/env_variables.h"
#include "gateway/server.h"

#include <signal.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, listen_addr, "0.0.0.0", "Address to listen");
ABSL_FLAG(int, engine_conn_port, 10007, "Port for engine connections");
ABSL_FLAG(int, http_port, 8080, "Port for HTTP connections");
ABSL_FLAG(int, grpc_port, 50051, "Port for gRPC connections");
ABSL_FLAG(int, num_io_workers, 1, "Number of IO workers.");
ABSL_FLAG(std::string, func_config_file, "", "Path to function config file");

static std::atomic<faas::gateway::Server*> server_ptr(nullptr);
void SignalHandlerToStopServer(int signal) {
    faas::gateway::Server* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

int main(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopServer);
    faas::base::InitMain(argc, argv);

    auto server = std::make_unique<faas::gateway::Server>();
    server->set_address(absl::GetFlag(FLAGS_listen_addr));
    server->set_engine_conn_port(absl::GetFlag(FLAGS_engine_conn_port));
    server->set_http_port(absl::GetFlag(FLAGS_http_port));
    server->set_grpc_port(absl::GetFlag(FLAGS_grpc_port));
    server->set_num_io_workers(absl::GetFlag(FLAGS_num_io_workers));
    server->set_func_config_file(absl::GetFlag(FLAGS_func_config_file));

    server->Start();
    server_ptr.store(server.get());
    server->WaitForFinish();

    return 0;
}
