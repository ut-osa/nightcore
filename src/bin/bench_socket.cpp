#include "base/init.h"
#include "base/common.h"
#include "common/time.h"
#include "utils/io.h"
#include "utils/socket.h"
#include "utils/bench.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <netinet/in.h> 

#include <absl/flags/flag.h>

ABSL_FLAG(std::string, socket_type, "unix", "tcp, tcp6, unix, or pipe");
ABSL_FLAG(size_t, payload_bytesize, 16, "Byte size of each payload");
ABSL_FLAG(int, tcp_port, 32767, "Port for TCP socket type");
ABSL_FLAG(int, server_cpu, -1, "Pin server process to this CPU");
ABSL_FLAG(int, client_cpu, -1, "Pin client process to this CPU");
ABSL_FLAG(absl::Duration, duration, absl::Seconds(30), "Duration to run");

using namespace faas;

static constexpr size_t kBufferSizeForSamples = 1<<24;

void Server(int infd, int outfd) {
    size_t payload_bytesize = absl::GetFlag(FLAGS_payload_bytesize);
    int cpu = absl::GetFlag(FLAGS_server_cpu);

    bench_utils::Samples<int32_t> msg_delay(kBufferSizeForSamples);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }
    char* payload_buffer = new char[payload_bytesize];
    auto perf_event_group = bench_utils::SetupCpuRelatedPerfEvents(cpu);
    perf_event_group->ResetAndEnable();

    bench_utils::BenchLoop bench_loop(absl::GetFlag(FLAGS_duration), [&] () -> bool {
        int64_t current_timestamp = GetMonotonicNanoTimestamp();
        memcpy(payload_buffer, &current_timestamp, sizeof(int64_t));
        CHECK(io_utils::SendData(outfd, payload_buffer, payload_bytesize));
        bool eof = false;
        CHECK(io_utils::RecvData(infd, payload_buffer, payload_bytesize, &eof));
        current_timestamp = GetMonotonicNanoTimestamp();
        int64_t send_timestamp;
        memcpy(&send_timestamp, payload_buffer, sizeof(int64_t));
        msg_delay.Add(gsl::narrow_cast<int32_t>(current_timestamp - send_timestamp));
        return true;
    });

    perf_event_group->Disable();

    // Signal client to stop
    int64_t value = -1;
    memcpy(payload_buffer, &value, sizeof(int64_t));
    CHECK(io_utils::SendData(outfd, payload_buffer, payload_bytesize));

    delete[] payload_buffer;
    LOG(INFO) << "Server: elapsed milliseconds: "
              << absl::ToInt64Milliseconds(bench_loop.elapsed_time());
    LOG(INFO) << "Server: loop rate: "
              << bench_loop.loop_count() / absl::ToDoubleMilliseconds(bench_loop.elapsed_time())
              << " loops per millisecond";
    msg_delay.ReportStatistics("Client message delay");
    bench_utils::ReportCpuRelatedPerfEventValues("Server", perf_event_group.get(),
                                                 bench_loop.elapsed_time(),
                                                 bench_loop.loop_count());

    PCHECK(close(infd) == 0);
    if (outfd != infd) {
        PCHECK(close(outfd) == 0);
    }
}

void Client(int infd, int outfd) {
    size_t payload_bytesize = absl::GetFlag(FLAGS_payload_bytesize);
    int cpu = absl::GetFlag(FLAGS_client_cpu);

    bench_utils::Samples<int32_t> msg_delay(kBufferSizeForSamples);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }
    char* payload_buffer = new char[payload_bytesize];

    bench_utils::BenchLoop bench_loop([&] () -> bool {
        bool eof = false;
        CHECK(io_utils::RecvData(infd, payload_buffer, payload_bytesize, &eof));
        int64_t current_timestamp = GetMonotonicNanoTimestamp();
        int64_t send_timestamp;
        memcpy(&send_timestamp, payload_buffer, sizeof(int64_t));
        if (send_timestamp == -1) {
            return false;
        }
        msg_delay.Add(gsl::narrow_cast<int32_t>(current_timestamp - send_timestamp));
        current_timestamp = GetMonotonicNanoTimestamp();
        memcpy(payload_buffer, &current_timestamp, sizeof(int64_t));
        CHECK(io_utils::SendData(outfd, payload_buffer, payload_bytesize));
        return true;
    });

    delete[] payload_buffer;
    LOG(INFO) << "Client: elapsed milliseconds: "
              << absl::ToInt64Milliseconds(bench_loop.elapsed_time());
    msg_delay.ReportStatistics("Server message delay");

    PCHECK(close(infd) == 0);
    if (outfd != infd) {
        PCHECK(close(outfd) == 0);
    }
}

int main(int argc, char* argv[]) {
    base::InitMain(argc, argv);

    int payload_bytesize = absl::GetFlag(FLAGS_payload_bytesize);
    CHECK_GE(payload_bytesize, 8) << "payload should be at least 8 bytes";

    std::string socket_type(absl::GetFlag(FLAGS_socket_type));
    int tcp_server_fd = -1;
    int unix_fds[2];
    int pipe1_fds[2];
    int pipe2_fds[2];
    if (socket_type == "unix") {
        PCHECK(socketpair(AF_LOCAL, SOCK_STREAM, 0, unix_fds) == 0);
    } else if (socket_type == "pipe") {
        PCHECK(pipe(pipe1_fds) == 0);
        PCHECK(pipe(pipe2_fds) == 0);
    } else if (socket_type == "tcp") {
        tcp_server_fd = utils::TcpSocketBindAndListen("127.0.0.1", absl::GetFlag(FLAGS_tcp_port));
    } else if (socket_type == "tcp6") {
        tcp_server_fd = utils::Tcp6SocketBindAndListen("::1", absl::GetFlag(FLAGS_tcp_port));
    } else {
        LOG(FATAL) << "Unsupported socket type: " << socket_type;
    }

    pid_t child_pid = fork();
    if (child_pid == 0) {
        int infd = -1;
        int outfd = -1;
        if (socket_type == "unix") {
            infd = outfd = unix_fds[0];
        } else if (socket_type == "pipe") {
            infd = pipe1_fds[0];
            outfd = pipe2_fds[1];
        } else if (socket_type == "tcp") {
            infd = outfd = utils::TcpSocketConnect("127.0.0.1", absl::GetFlag(FLAGS_tcp_port));
        } else if (socket_type == "tcp6") {
            infd = outfd = utils::Tcp6SocketConnect("::1", absl::GetFlag(FLAGS_tcp_port));
        }
        Client(infd, outfd);
        return 0;
    }

    PCHECK(child_pid != -1);
    int infd = -1;
    int outfd = -1;
    if (socket_type == "unix") {
        infd = outfd = unix_fds[1];
    } else if (socket_type == "pipe") {
        infd = pipe2_fds[0];
        outfd = pipe1_fds[1];
    } else if (socket_type == "tcp") {
        struct sockaddr_in addr;
        socklen_t addr_len = sizeof(addr);
        int fd = accept(tcp_server_fd, (struct sockaddr*)&addr, &addr_len);
        PCHECK(fd != -1);
        infd = outfd = fd;
    } else if (socket_type == "tcp6") {
        struct sockaddr_in6 addr;
        socklen_t addr_len = sizeof(addr);
        int fd = accept(tcp_server_fd, (struct sockaddr*)&addr, &addr_len);
        PCHECK(fd != -1);
        infd = outfd = fd;
    }
    Server(infd, outfd);

    int wstatus;
    CHECK(wait(&wstatus) == child_pid);

    return 0;
}
