#include "base/init.h"
#include "base/common.h"
#include "common/time.h"
#include "utils/fs.h"
#include "utils/bench.h"

#include <absl/flags/flag.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

ABSL_FLAG(int, cpu, 0, "Pin benchmark thread to this CPU");
ABSL_FLAG(std::string, shm_base_path, "/dev/shm/test",
          "Base path for shared memory files");
ABSL_FLAG(size_t, shm_size, 4096, "Shared memory size");
ABSL_FLAG(absl::Duration, duration, absl::Seconds(30), "Duration to run");

using namespace faas;

static constexpr size_t kBufferSizeForSamples = 1<<24;

int main(int argc, char* argv[]) {
    base::InitMain(argc, argv);

    std::string shm_base_path(absl::GetFlag(FLAGS_shm_base_path));
    if (fs_utils::Exists(shm_base_path)) {
        if (fs_utils::IsDirectory(shm_base_path)) {
            fs_utils::RemoveDirectoryRecursively(shm_base_path);
        } else {
            fs_utils::Remove(shm_base_path);
        }
    }
    fs_utils::MakeDirectory(shm_base_path);

    bench_utils::Samples<int32_t> file_creation_delay(kBufferSizeForSamples);
    bench_utils::Samples<int32_t> ftruncate_delay(kBufferSizeForSamples);
    bench_utils::Samples<int32_t> mmap_delay(kBufferSizeForSamples);
    bench_utils::Samples<int32_t> close_delay(kBufferSizeForSamples);
    bench_utils::Samples<int32_t> munmap_delay(kBufferSizeForSamples);

    size_t shm_size = absl::GetFlag(FLAGS_shm_size);
    int cpu = absl::GetFlag(FLAGS_cpu);
    if (cpu != -1) {
        bench_utils::PinCurrentThreadToCpu(cpu);
    }
    auto perf_event_group = bench_utils::SetupCpuRelatedPerfEvents(cpu);
    perf_event_group->ResetAndEnable();

    int index = 0;
    bench_utils::BenchLoop bench_loop(absl::GetFlag(FLAGS_duration), [&] () -> bool {
        int64_t start_timestamp;
        std::string path = fmt::format("{}/{}", shm_base_path, index++);
        // File creation
        start_timestamp = GetMonotonicNanoTimestamp();
        int fd = open(path.c_str(), O_CREAT|O_TRUNC|O_RDWR, 0600);
        file_creation_delay.Add(
            gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - start_timestamp));
        PCHECK(fd != -1);
        // Ftruncate
        start_timestamp = GetMonotonicNanoTimestamp();
        PCHECK(ftruncate(fd, shm_size) == 0);
        ftruncate_delay.Add(
            gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - start_timestamp));
        // Mmap
        start_timestamp = GetMonotonicNanoTimestamp();
        void* ptr = mmap(0, shm_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
        mmap_delay.Add(
            gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - start_timestamp));
        CHECK(ptr != MAP_FAILED);
        // Close file
        start_timestamp = GetMonotonicNanoTimestamp();
        close(fd);
        close_delay.Add(
            gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - start_timestamp));
        // Write data
        memset(ptr, 0, shm_size);
        // Munmap
        start_timestamp = GetMonotonicNanoTimestamp();
        PCHECK(munmap(ptr, shm_size) == 0);
        munmap_delay.Add(
            gsl::narrow_cast<int32_t>(GetMonotonicNanoTimestamp() - start_timestamp));
        return true;
    });

    perf_event_group->Disable();
    fs_utils::RemoveDirectoryRecursively(shm_base_path);

    LOG(INFO) << "Elapsed milliseconds: "
              << absl::ToInt64Milliseconds(bench_loop.elapsed_time());
    LOG(INFO) << "Loop rate: "
              << bench_loop.loop_count() / absl::ToDoubleMilliseconds(bench_loop.elapsed_time())
              << " loops per millisecond";
    file_creation_delay.ReportStatistics("File creation delay");
    ftruncate_delay.ReportStatistics("Ftruncate delay");
    mmap_delay.ReportStatistics("Mmap delay");
    close_delay.ReportStatistics("Close delay");
    munmap_delay.ReportStatistics("Munmap delay");
    bench_utils::ReportCpuRelatedPerfEventValues("Shm", perf_event_group.get(),
                                                 bench_loop.elapsed_time(),
                                                 bench_loop.loop_count());

    return 0;
}
