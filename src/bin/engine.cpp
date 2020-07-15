#include "base/init.h"
#include "base/common.h"
#include "ipc/base.h"
#include "utils/docker.h"
#include "utils/fs.h"
#include "utils/env_variables.h"
#include "engine/engine.h"

#include <signal.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, gateway_addr, "127.0.0.1", "Gateway address");
ABSL_FLAG(int, gateway_port, 10007, "Gataway port");
ABSL_FLAG(int, engine_tcp_port, -1,
          "If set, Launcher and FuncWorker will communicate with engine via localhost TCP socket");
ABSL_FLAG(int, num_io_workers, 1, "Number of IO workers.");
ABSL_FLAG(int, gateway_conn_per_worker, 2, "Number of gateway connections per IO worker.");
ABSL_FLAG(int, node_id, -1,
          "My node ID. If -1 is set, node ID will be automatically generated based on "
          "/proc/sys/kernel/hostname");
ABSL_FLAG(std::string, root_path_for_ipc, "/dev/shm/faas_ipc",
          "Root directory for IPCs used by FaaS");
ABSL_FLAG(std::string, func_config_file, "", "Path to function config file");

static std::atomic<faas::engine::Engine*> engine_ptr(nullptr);
static void SignalHandlerToStopEngine(int signal) {
    faas::engine::Engine* engine = engine_ptr.exchange(nullptr);
    if (engine != nullptr) {
        engine->ScheduleStop();
    }
}

static uint16_t GenerateNodeId() {
    std::string hostname;
    if (!faas::fs_utils::ReadContents("/proc/sys/kernel/hostname", &hostname)) {
        LOG(FATAL) << "Failed to read /proc/sys/kernel/hostname";
    }
    uint16_t result = 0;
    for (const char ch : hostname) {
        // Let overflow happens freely here
        result = result * 177 + ch;
    }
    return result;
}

int main(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopEngine);
    faas::base::InitMain(argc, argv);
    faas::ipc::SetRootPathForIpc(absl::GetFlag(FLAGS_root_path_for_ipc), /* create= */ true);

    std::string cgroup_fs_root(faas::utils::GetEnvVariable("FAAS_CGROUP_FS_ROOT", ""));
    if (cgroup_fs_root.length() > 0) {
        faas::docker_utils::SetCgroupFsRoot(cgroup_fs_root);
    }

    auto engine = std::make_unique<faas::engine::Engine>();
    engine->set_gateway_addr_port(absl::GetFlag(FLAGS_gateway_addr),
                                  absl::GetFlag(FLAGS_gateway_port));
    engine->set_engine_tcp_port(absl::GetFlag(FLAGS_engine_tcp_port));
    engine->set_num_io_workers(absl::GetFlag(FLAGS_num_io_workers));
    engine->set_gateway_conn_per_worker(absl::GetFlag(FLAGS_gateway_conn_per_worker));
    int node_id = absl::GetFlag(FLAGS_node_id);
    if (node_id == -1) {
        engine->set_node_id(GenerateNodeId());
    } else {
        engine->set_node_id(gsl::narrow_cast<uint16_t>(node_id));
    }
    engine->set_func_config_file(absl::GetFlag(FLAGS_func_config_file));

    engine->Start();
    engine_ptr.store(engine.get());
    engine->WaitForFinish();

    return 0;
}
