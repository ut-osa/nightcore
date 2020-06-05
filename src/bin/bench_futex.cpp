#include "base/init.h"
#include "base/common.h"
#include "common/time.h"
#include "utils/bench.h"

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <linux/futex.h>

#include <absl/flags/flag.h>

ABSL_FLAG(int, server_cpu, -1, "Pin server process to this CPU");
ABSL_FLAG(int, client_cpu, -1, "Pin client process to this CPU");
ABSL_FLAG(absl::Duration, duration, absl::Seconds(30), "Duration to run");
ABSL_FLAG(std::string, shm_base_path, "/dev/shm/faas", "Base path for shared memory");

static int futex(int* uaddr, int futex_op, int val,
                 const struct timespec* timeout, int* uaddr2, int val3) {
    return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}

static void futex_wait(int* futex_ptr) {
    while (true) {
        int one = 1;
        int zero = 0;
        if (__atomic_compare_exchange(futex_ptr, &one, &zero,
                                      false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
            break;
        }
        int ret = futex(futex_ptr, FUTEX_WAIT, 0, NULL, NULL, 0);
        PCHECK(ret != -1 || errno == EAGAIN) << "futex wait failed";
    }
}

static void futex_post(int* futex_ptr) {
    int one = 1;
    int zero = 0;
    if (__atomic_compare_exchange(futex_ptr, &zero, &one,
                                  false, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST)) {
        int ret = futex(futex_ptr, FUTEX_WAKE, 1, NULL, NULL, 0);
        PCHECK(ret != -1) << "futex wake failed";
    }
}

using namespace faas;

static constexpr size_t kBufferSizeForSamples = 1<<24;
static constexpr int64_t kFutexStopValue = -1;

void Server(int* futex1, int* futex2, int64_t* timestamp1_ptr, int64_t* timestamp2_ptr) {
    int cpu = absl::GetFlag(FLAGS_server_cpu);
    bench_utils::Samples<int32_t> futex_delay(kBufferSizeForSamples);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }

    auto perf_event_group = bench_utils::SetupCpuRelatedPerfEvents(cpu);
    perf_event_group->ResetAndEnable();

    bench_utils::BenchLoop bench_loop(absl::GetFlag(FLAGS_duration), [&] () -> bool {
        int64_t current_timestamp = GetMonotonicNanoTimestamp();
        __atomic_store_n(timestamp1_ptr, current_timestamp, __ATOMIC_RELEASE);
        futex_post(futex1);
        futex_wait(futex2);
        current_timestamp = GetMonotonicNanoTimestamp();
        int64_t send_timestamp = __atomic_load_n(timestamp2_ptr, __ATOMIC_ACQUIRE);
        futex_delay.Add(gsl::narrow_cast<int32_t>(current_timestamp - send_timestamp));
        return true;
    });

    perf_event_group->Disable();

    // Signal client to stop
    __atomic_store_n(timestamp1_ptr, kFutexStopValue, __ATOMIC_RELEASE);
    futex_post(futex1);

    LOG(INFO) << "Server: elapsed milliseconds: "
              << absl::ToInt64Milliseconds(bench_loop.elapsed_time());
    LOG(INFO) << "Server: loop rate: "
              << bench_loop.loop_count() / absl::ToDoubleMilliseconds(bench_loop.elapsed_time())
              << " loops per millisecond";
    futex_delay.ReportStatistics("Client futex delay");
    bench_utils::ReportCpuRelatedPerfEventValues("Server", perf_event_group.get(),
                                                 bench_loop.elapsed_time(),
                                                 bench_loop.loop_count());
}

void Client(int* futex1, int* futex2, int64_t* timestamp1_ptr, int64_t* timestamp2_ptr) {
    int cpu = absl::GetFlag(FLAGS_client_cpu);
    bench_utils::Samples<int32_t> futex_delay(kBufferSizeForSamples);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }

    bench_utils::BenchLoop bench_loop([&] () -> bool {
        futex_wait(futex1);
        int64_t send_timestamp = __atomic_load_n(timestamp1_ptr, __ATOMIC_ACQUIRE);
        if (send_timestamp == kFutexStopValue) {
            return false;
        }
        int64_t current_timestamp = GetMonotonicNanoTimestamp();
        futex_delay.Add(gsl::narrow_cast<int32_t>(current_timestamp - send_timestamp));
        __atomic_store_n(timestamp2_ptr, current_timestamp, __ATOMIC_RELEASE);
        futex_post(futex2);
        return true;
    });

    LOG(INFO) << "Client: elapsed milliseconds: "
              << absl::ToInt64Milliseconds(bench_loop.elapsed_time());
    futex_delay.ReportStatistics("Server futex delay");
}

int main(int argc, char* argv[]) {
    base::InitMain(argc, argv);

    char* shm_base = reinterpret_cast<char*>(mmap(
        nullptr, __FAAS_PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_SHARED, -1, 0));
    PCHECK(shm_base != MAP_FAILED);

    int* futex1 = reinterpret_cast<int*>(shm_base);
    int* futex2 = reinterpret_cast<int*>(shm_base + __FAAS_CACHE_LINE_SIZE);
    int64_t* timestamp1_ptr = reinterpret_cast<int64_t*>(shm_base + __FAAS_CACHE_LINE_SIZE * 2);
    int64_t* timestamp2_ptr = reinterpret_cast<int64_t*>(shm_base + __FAAS_CACHE_LINE_SIZE * 3);

    *futex1 = 0;
    *futex2 = 1;

    pid_t child_pid = fork();
    if (child_pid == 0) {
        Client(futex1, futex2, timestamp1_ptr, timestamp2_ptr);
        return 0;
    }

    PCHECK(child_pid != -1);
    Server(futex1, futex2, timestamp1_ptr, timestamp2_ptr);

    int wstatus;
    CHECK(wait(&wstatus) == child_pid);

    return 0;
}
