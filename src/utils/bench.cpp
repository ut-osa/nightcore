#include "utils/bench.h"

#include "common/time.h"
#include "utils/env_variables.h"

#include <sched.h>
#include <sys/syscall.h>

namespace faas {
namespace bench_utils {

void PinCurrentThreadToCpu(int cpu) {
    pid_t tid = syscall(SYS_gettid);
    LOG(INFO) << "Pin thread (tid=" << tid << ") to CPU " << cpu;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    PCHECK(sched_setaffinity(0, sizeof(set), &set) == 0);
}

std::unique_ptr<utils::PerfEventGroup> SetupCpuRelatedPerfEvents(int cpu) {
    auto perf_event_group = std::make_unique<utils::PerfEventGroup>();
    if (cpu != -1) {
        perf_event_group->set_cpu(cpu);
    }
    if (utils::GetEnvVariableAsInt("PERF_EVENT_KERNEL_ONLY", 0)) {
        perf_event_group->set_exclude_user(true);
    } else if (utils::GetEnvVariableAsInt("PERF_EVENT_USER_ONLY", 0)) {
        perf_event_group->set_exclude_kernel(true);
    }
    CHECK(perf_event_group->AddEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES))
        << "Failed to add PERF_COUNT_HW_CPU_CYCLES event";
    CHECK(perf_event_group->AddEvent(PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS))
        << "Failed to add PERF_COUNT_HW_INSTRUCTIONS event";
    return perf_event_group;
}

void ReportCpuRelatedPerfEventValues(std::string_view header,
                                     utils::PerfEventGroup* perf_event_group,
                                     absl::Duration duration, size_t loop_count) {
    std::vector<uint64_t> values = perf_event_group->ReadValues();
    LOG(INFO) << header << ": value of PERF_COUNT_HW_CPU_CYCLES: " << values[0];
    LOG(INFO) << header << ": value of PERF_COUNT_HW_INSTRUCTIONS: " << values[1];
    LOG(INFO) << header << ": rate of PERF_COUNT_HW_CPU_CYCLES: "
              << values[0] / absl::ToDoubleMicroseconds(duration) << " per us, "
              << gsl::narrow_cast<double>(values[0]) / loop_count << " per loop";
    LOG(INFO) << header << ": rate of PERF_COUNT_HW_INSTRUCTIONS: "
              << values[1] / absl::ToDoubleMicroseconds(duration) << " per us, "
              << gsl::narrow_cast<double>(values[1]) / loop_count << " per loop";
}

BenchLoop::BenchLoop(LoopFn fn)
    : fn_(fn), max_duration_(absl::InfiniteDuration()),
      max_loop_count_(std::numeric_limits<size_t>::max()),
      finished_(false) {
    Run();
}

BenchLoop::BenchLoop(absl::Duration max_duration, LoopFn fn)
    : fn_(fn), max_duration_(max_duration),
      max_loop_count_(std::numeric_limits<size_t>::max()),
      finished_(false) {
    Run();
}

BenchLoop::BenchLoop(size_t max_loop_count, LoopFn fn)
    : fn_(fn), max_duration_(absl::InfiniteDuration()),
      max_loop_count_(max_loop_count), finished_(false) {
    Run();
}

BenchLoop::~BenchLoop() {
    CHECK(finished_);
}

void BenchLoop::Run() {
    CHECK(!finished_);
    int64_t start_timestamp = GetMonotonicNanoTimestamp();
    int64_t stop_timestamp = std::numeric_limits<int64_t>::max();
    if (max_duration_ != absl::InfiniteDuration()) {
        stop_timestamp = start_timestamp + absl::ToInt64Nanoseconds(max_duration_);
    }
    loop_count_ = 0;
    do {
        if (loop_count_ >= max_loop_count_ || GetMonotonicNanoTimestamp() >= stop_timestamp) {
            break;
        }
        loop_count_++;
        if (!fn_()) {
            break;
        }
    } while (true);
    duration_ = absl::Nanoseconds(GetMonotonicNanoTimestamp() - start_timestamp);
    finished_ = true;
}

absl::Duration BenchLoop::elapsed_time() const {
    CHECK(finished_);
    return duration_;
}

size_t BenchLoop::loop_count() const {
    CHECK(finished_);
    return loop_count_;
}

}  // namespace bench_utils
}  // namespace faas
