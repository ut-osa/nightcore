#include "base/init.h"
#include "base/common.h"
#include "common/time.h"
#include "utils/bench.h"

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/eventfd.h>

#include <absl/flags/flag.h>

ABSL_FLAG(int, server_cpu, -1, "Pin server process to this CPU");
ABSL_FLAG(int, client_cpu, -1, "Pin client process to this CPU");
ABSL_FLAG(absl::Duration, duration, absl::Seconds(30), "Duration to run");

using namespace faas;

static constexpr size_t kBufferSizeForSamples = 1<<24;
static constexpr uint64_t kEventFdStopValue = 0xfffffffffffffffeULL;

void Server(int infd, int outfd) {
    int cpu = absl::GetFlag(FLAGS_server_cpu);
    bench_utils::Samples<int32_t> eventfd_delay(kBufferSizeForSamples);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }

    auto perf_event_group = bench_utils::SetupCpuRelatedPerfEvents(cpu);
    perf_event_group->ResetAndEnable();

    bench_utils::BenchLoop bench_loop(absl::GetFlag(FLAGS_duration), [&] () -> bool {
        int64_t current_timestamp = GetMonotonicNanoTimestamp();
        uint64_t value = current_timestamp;
        PCHECK(eventfd_write(outfd, value) == 0) << "eventfd_write failed";
        PCHECK(eventfd_read(infd, &value) == 0) << "eventfd_read failed";
        current_timestamp = GetMonotonicNanoTimestamp();
        int64_t send_timestamp = gsl::narrow_cast<int64_t>(value);
        eventfd_delay.Add(gsl::narrow_cast<int32_t>(current_timestamp - send_timestamp));
        return true;
    });

    perf_event_group->Disable();

    // Signal client to stop
    PCHECK(eventfd_write(outfd, kEventFdStopValue) == 0) << "eventfd_write failed";

    LOG(INFO) << "Server: elapsed milliseconds: "
              << absl::ToInt64Milliseconds(bench_loop.elapsed_time());
    LOG(INFO) << "Server: loop rate: "
              << bench_loop.loop_count() / absl::ToDoubleMilliseconds(bench_loop.elapsed_time())
              << " loops per millisecond";
    eventfd_delay.ReportStatistics("Client eventfd delay");
    bench_utils::ReportCpuRelatedPerfEventValues("Server", perf_event_group.get(),
                                                 bench_loop.elapsed_time(),
                                                 bench_loop.loop_count());

    PCHECK(close(infd) == 0);
    PCHECK(close(outfd) == 0);
}

void Client(int infd, int outfd) {
    int cpu = absl::GetFlag(FLAGS_client_cpu);
    bench_utils::Samples<int32_t> eventfd_delay(kBufferSizeForSamples);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }

    bench_utils::BenchLoop bench_loop([&] () -> bool {
        uint64_t value;
        PCHECK(eventfd_read(infd, &value) == 0) << "eventfd_read failed";
        if (value == kEventFdStopValue) {
            return false;
        }
        int64_t current_timestamp = GetMonotonicNanoTimestamp();
        int64_t send_timestamp = gsl::narrow_cast<int64_t>(value);
        eventfd_delay.Add(gsl::narrow_cast<int32_t>(current_timestamp - send_timestamp));
        value = current_timestamp;
        PCHECK(eventfd_write(outfd, value) == 0) << "eventfd_write failed";
        return true;
    });

    LOG(INFO) << "Client: elapsed milliseconds: "
              << absl::ToInt64Milliseconds(bench_loop.elapsed_time());
    eventfd_delay.ReportStatistics("Server eventfd delay");

    PCHECK(close(infd) == 0);
    PCHECK(close(outfd) == 0);
}

int main(int argc, char* argv[]) {
    base::InitMain(argc, argv);

    int fd1 = eventfd(0, 0);
    PCHECK(fd1 != -1);
    int fd2 = eventfd(0, 0);
    PCHECK(fd2 != -1);

    pid_t child_pid = fork();
    if (child_pid == 0) {
        Client(fd2, fd1);
        return 0;
    }

    PCHECK(child_pid != -1);
    Server(fd1, fd2);

    int wstatus;
    CHECK(wait(&wstatus) == child_pid);

    return 0;
}
