#include "base/init.h"
#include "base/common.h"
#include "common/time.h"
#include "utils/fs.h"
#include "utils/bench.h"

#include <absl/flags/flag.h>

#include <sys/types.h>
#include <sys/shm.h>
#include <fcntl.h>

ABSL_FLAG(int, cpu, 0, "Pin benchmark thread to this CPU");
ABSL_FLAG(size_t, shm_size, 4096, "Shared memory size");
ABSL_FLAG(absl::Duration, duration, absl::Seconds(30), "Duration to run");

using namespace faas;

static constexpr size_t kBufferSizeForSamples = 1<<24;

int main(int argc, char* argv[]) {
    base::InitMain(argc, argv);

    bench_utils::Samples<int32_t> shmget_delay(kBufferSizeForSamples);
    bench_utils::Samples<int32_t> shmat_delay(kBufferSizeForSamples);
    bench_utils::Samples<int32_t> shmdt_delay(kBufferSizeForSamples);
    bench_utils::Samples<int32_t> shm_remove_delay(kBufferSizeForSamples);

    size_t shm_size = absl::GetFlag(FLAGS_shm_size);
    int cpu = absl::GetFlag(FLAGS_cpu);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }
    auto perf_event_group = bench_utils::SetupCpuRelatedPerfEvents(cpu);
    perf_event_group->ResetAndEnable();

    bench_utils::BenchLoop bench_loop(absl::GetFlag(FLAGS_duration), [&] () -> bool {
        int64_t start_timestamp;
        // shmget
        start_timestamp = GetMonotonicNanoTimestamp();
        int shmid = shmget(IPC_PRIVATE, shm_size, IPC_CREAT | 0600);
        shmget_delay.Add(
            gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - start_timestamp));
        PCHECK(shmid != -1);
        // shmat
        start_timestamp = GetMonotonicNanoTimestamp();
        void* ptr = shmat(shmid, nullptr, 0);
        shmat_delay.Add(
            gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - start_timestamp));
        PCHECK(ptr != (void*)-1);
        // Write data
        memset(ptr, 0, shm_size);
        // shmdt
        start_timestamp = GetMonotonicNanoTimestamp();
        PCHECK(shmdt(ptr) == 0);
        shmdt_delay.Add(
            gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - start_timestamp));
        // shmctl remove
        start_timestamp = GetMonotonicNanoTimestamp();
        PCHECK(shmctl(shmid, IPC_RMID, nullptr) == 0);
        shm_remove_delay.Add(
            gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - start_timestamp));
        return true;
    });

    perf_event_group->Disable();

    LOG(INFO) << "Elapsed milliseconds: "
              << absl::ToInt64Milliseconds(bench_loop.elapsed_time());
    LOG(INFO) << "Loop rate: "
              << bench_loop.loop_count() / absl::ToDoubleMilliseconds(bench_loop.elapsed_time())
              << " loops per millisecond";
    shmget_delay.ReportStatistics("shmget delay");
    shmat_delay.ReportStatistics("shmat delay");
    shmdt_delay.ReportStatistics("shmdt delay");
    shm_remove_delay.ReportStatistics("shm_remove delay");
    bench_utils::ReportCpuRelatedPerfEventValues("Shm", perf_event_group.get(),
                                                 bench_loop.elapsed_time(),
                                                 bench_loop.loop_count());

    return 0;
}
