#include "base/init.h"
#include "base/common.h"
#include "ipc/base.h"
#include "utils/docker.h"
#include "utils/env_variables.h"
#include "gateway/server.h"

#include <signal.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, listen_addr, "0.0.0.0", "Address to listen");
ABSL_FLAG(int, http_port, 8080, "Port to listen");
ABSL_FLAG(int, grpc_port, -1, "Port for gRPC services");
ABSL_FLAG(int, num_http_workers, 1, "Number of HTTP workers");
ABSL_FLAG(int, num_ipc_workers, 1, "Number of IPC workers");
ABSL_FLAG(int, num_io_workers, -1,
          "Number of IO workers. If set, gateway will not separate HTTP and IPC workers, "
          "i.e. --num_http_workers and --num_ipc_workers will both be ignored.");
ABSL_FLAG(std::string, root_path_for_ipc, "/dev/shm/faas_ipc",
          "Root directory for IPCs used by FaaS");
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
    faas::ipc::SetRootPathForIpc(absl::GetFlag(FLAGS_root_path_for_ipc), /* create= */ true);

    std::string cgroup_fs_root(faas::utils::GetEnvVariable("FAAS_CGROUP_FS_ROOT", ""));
    if (cgroup_fs_root.length() > 0) {
        faas::docker_utils::SetCgroupFsRoot(cgroup_fs_root);
    }

    auto server = std::make_unique<faas::gateway::Server>();
    server->set_address(absl::GetFlag(FLAGS_listen_addr));
    server->set_http_port(absl::GetFlag(FLAGS_http_port));
    server->set_grpc_port(absl::GetFlag(FLAGS_grpc_port));
    server->set_num_http_workers(absl::GetFlag(FLAGS_num_http_workers));
    server->set_num_ipc_workers(absl::GetFlag(FLAGS_num_ipc_workers));
    server->set_num_io_workers(absl::GetFlag(FLAGS_num_io_workers));
    server->set_func_config_file(absl::GetFlag(FLAGS_func_config_file));

    server->Start();
    server_ptr.store(server.get());
    server->WaitForFinish();

    return 0;
}
