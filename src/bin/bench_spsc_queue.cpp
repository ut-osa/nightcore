#include "base/init.h"
#include "base/asm.h"
#include "base/common.h"
#include "common/time.h"
#include "utils/bench.h"
#include "ipc/spsc_queue.h"

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/eventfd.h>

#include <absl/flags/flag.h>

ABSL_FLAG(std::string, root_path_for_ipc, "/dev/shm/faas_ipc",
          "Root directory for IPCs used by FaaS");
ABSL_FLAG(int, server_cpu, -1, "Pin server process to this CPU");
ABSL_FLAG(int, client_cpu, -1, "Pin client process to this CPU");
ABSL_FLAG(absl::Duration, duration, absl::Seconds(30), "Duration to run");
ABSL_FLAG(size_t, server_queue_sleep_every, 0,
          "Server queue will sleep based on this interval");

using namespace faas;

static constexpr size_t kQueueSize = 1024;
static constexpr size_t kBufferSizeForSamples = 1<<26;
static constexpr uint64_t kEventServerQueueCreated = 1;
static constexpr uint64_t kEventClientQueueCreated = 2;
static constexpr uint64_t kEventServerReady = 4;
static constexpr uint64_t kEventWakeupServerQueue = 5;

void Server(int infd, int outfd) {
    int cpu = absl::GetFlag(FLAGS_server_cpu);
    size_t sleep_every = absl::GetFlag(FLAGS_server_queue_sleep_every);
    bench_utils::Samples<int32_t> msg_delay(kBufferSizeForSamples);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }
    auto perf_event_group = bench_utils::SetupCpuRelatedPerfEvents(cpu);

    auto server_queue = ipc::SPSCQueue<int64_t>::Create("server", kQueueSize);
    PCHECK(eventfd_write(outfd, kEventServerQueueCreated) == 0) << "eventfd_write failed";
    uint64_t event_value;
    PCHECK(eventfd_read(infd, &event_value) == 0) << "eventfd_read failed";
    CHECK_EQ(event_value, kEventClientQueueCreated);
    auto client_queue = ipc::SPSCQueue<int64_t>::Open("client");
    PCHECK(eventfd_write(outfd, kEventServerReady) == 0) << "eventfd_write failed";

    perf_event_group->ResetAndEnable();
    size_t recv_message_count = 0;
    bool slept = false;
    bench_utils::BenchLoop bench_loop(absl::GetFlag(FLAGS_duration), [&] () -> bool {
        int64_t send_value = GetMonotonicNanoTimestamp();
        do {
            if (client_queue->Push(send_value)) {
                break;
            }
            asm_volatile_pause();
        } while (true);
        if (slept) {
            uint64_t event_value;
            PCHECK(eventfd_read(infd, &event_value) == 0) << "eventfd_read failed";
            CHECK_EQ(event_value, kEventWakeupServerQueue);
            slept = false;
        }
        int64_t recv_value;
        do {
            if (server_queue->Pop(&recv_value)) {
                break;
            }
            asm_volatile_pause();
        } while (true);
        int64_t send_timestamp = recv_value;
        msg_delay.Add(gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - send_timestamp));
        recv_message_count++;
        if (sleep_every > 0 && recv_message_count % sleep_every == 0) {
            server_queue->ConsumerEnterSleep();
            slept = true;
        }
        return true;
    });

    perf_event_group->Disable();

    // Signal client to stop
    do {
        if (client_queue->Push(-1)) {
            break;
        }
        asm_volatile_pause();
    } while (true);

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
    PCHECK(close(outfd) == 0);
}

void Client(int infd, int outfd) {
    int cpu = absl::GetFlag(FLAGS_client_cpu);
    bench_utils::Samples<int32_t> msg_delay(kBufferSizeForSamples);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }

    uint64_t event_value;
    PCHECK(eventfd_read(infd, &event_value) == 0) << "eventfd_read failed";
    CHECK_EQ(event_value, kEventServerQueueCreated);
    auto client_queue = ipc::SPSCQueue<int64_t>::Create("client", kQueueSize);
    auto server_queue = ipc::SPSCQueue<int64_t>::Open("server");
    server_queue->SetWakeupConsumerFn([outfd] () {
        PCHECK(eventfd_write(outfd, kEventWakeupServerQueue) == 0) << "eventfd_write failed";
    });
    PCHECK(eventfd_write(outfd, kEventClientQueueCreated) == 0) << "eventfd_write failed";
    PCHECK(eventfd_read(infd, &event_value) == 0) << "eventfd_read failed";
    CHECK_EQ(event_value, kEventServerReady);

    bench_utils::BenchLoop bench_loop([&] () -> bool {
        int64_t recv_value;
        do {
            if (client_queue->Pop(&recv_value)) {
                break;
            }
            asm_volatile_pause();
        } while (true);
        if (recv_value == -1) {
            return false;
        }
        int64_t current_timestamp = GetMonotonicNanoTimestamp();
        int64_t send_timestamp = recv_value;
        msg_delay.Add(gsl::narrow_cast<int32_t>(current_timestamp - send_timestamp));
        int64_t send_value = current_timestamp;
        do {
            if (server_queue->Push(send_value)) {
                break;
            }
            asm_volatile_pause();
        } while (true);
        return true;
    });

    LOG(INFO) << "Client: elapsed milliseconds: "
              << absl::ToInt64Milliseconds(bench_loop.elapsed_time());
    msg_delay.ReportStatistics("Server message delay");

    PCHECK(close(infd) == 0);
    PCHECK(close(outfd) == 0);
}

int main(int argc, char* argv[]) {
    base::InitMain(argc, argv);
    ipc::SetRootPathForIpc(absl::GetFlag(FLAGS_root_path_for_ipc), /* create= */ true);

    int fd1 = eventfd(0, 0);
    PCHECK(fd1 != -1);
    int fd2 = eventfd(0, 0);
    PCHECK(fd2 != -1);

    pid_t child_pid = fork();
    if (child_pid == 0) {
        Client(fd1, fd2);
        return 0;
    }

    PCHECK(child_pid != -1);
    Server(fd2, fd1);

    int wstatus;
    CHECK(wait(&wstatus) == child_pid);

    return 0;
}
