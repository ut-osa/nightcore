#include "base/init.h"
#include "base/common.h"
#include "gateway/server.h"

#include <thread>

#include <signal.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, listen_addr, "0.0.0.0", "Address to listen");
ABSL_FLAG(int, listen_port, 8080, "Port to listen");
ABSL_FLAG(int, num_http_workers, 2, "Number of HTTP workers");
ABSL_FLAG(int, num_ipc_workers, 2, "Number of IPC workers");
ABSL_FLAG(std::string, ipc_path, "/tmp/faas_gateway",
          "Domain socket path for IPC with watchdog processes");

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
    server->set_ipc_path(absl::GetFlag(FLAGS_ipc_path));
    server->set_port(absl::GetFlag(FLAGS_listen_port));
    server->set_num_http_workers(absl::GetFlag(FLAGS_num_http_workers));
    server->set_num_ipc_workers(absl::GetFlag(FLAGS_num_ipc_workers));

    // server->RegisterSyncRequestHandler(
    //     [] (absl::string_view method, absl::string_view path) -> bool {
    //         return method == "GET" && path == "/hello";
    //     },
    //     [] (faas::gateway::SyncRequestContext* context) {
    //         context->AppendStrToResponseBody("hello\n");
    //     });
    
    server->RegisterAsyncRequestHandler(
        [] (absl::string_view method, absl::string_view  path) -> bool {
            return method == "GET" && path == "/hello";
        },
        [] (std::shared_ptr<faas::gateway::HttpAsyncRequestContext> context) {
            std::thread thread([context] () {
                context->AppendStrToResponseBody("hello\n");
                context->Finish();
            });
            thread.detach();
        });

    server->RegisterSyncRequestHandler(
        [] (absl::string_view method, absl::string_view  path) -> bool {
            return method == "POST" && path == "/shutdown";
        },
        [&server] (faas::gateway::HttpSyncRequestContext* context) {
            context->AppendStrToResponseBody("Server is shutting down\n");
            server->ScheduleStop();
        });

    server->Start();
    server_ptr = server.get();
    signal(SIGINT, SignalHandlerToCloseServer);
    signal(SIGTERM, SignalHandlerToCloseServer);

    server->WaitForFinish();

    return 0;
}
