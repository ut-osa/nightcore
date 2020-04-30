#pragma once

#ifndef __FAAS_SRC
#error utils/bench.h cannot be included outside
#endif

#include "base/common.h"
#include "utils/perf_event.h"

namespace faas {
namespace bench_utils {

void PinCurrentThreadToCpu(int cpu);

std::unique_ptr<utils::PerfEventGroup> SetupCpuRelatedPerfEvents(int cpu = -1);
void ReportCpuRelatedPerfEventValues(std::string_view header,
                                     utils::PerfEventGroup* perf_event_group,
                                     absl::Duration duration, size_t loop_count);

class BenchLoop {
public:
    typedef std::function<bool()> LoopFn;

    explicit BenchLoop(LoopFn fn);
    BenchLoop(absl::Duration max_duration, LoopFn fn);
    BenchLoop(size_t max_loop_count, LoopFn fn);
    ~BenchLoop();

    absl::Duration elapsed_time() const;
    size_t loop_count() const;

private:
    LoopFn fn_;
    absl::Duration max_duration_;
    size_t max_loop_count_;

    bool finished_;
    absl::Duration duration_;
    size_t loop_count_;

    void Run();
    DISALLOW_COPY_AND_ASSIGN(BenchLoop);
};

template<class T>
class Samples {
public:
    explicit Samples(size_t buffer_size)
        : buffer_size_(buffer_size),
          buffer_(new T[buffer_size]),
          count_(0), pos_(0) {}

    ~Samples() { delete[] buffer_; }

    void Add(T value) {
        count_++;
        pos_++;
        if (pos_ == buffer_size_) {
            LOG(WARNING) << "Internal buffer of Samples not big enough";
            pos_ = 0;
        }
        buffer_[pos_] = value;
    }

    size_t count() const { return count_; }

    void ReportStatistics(std::string_view header) {
        size_t size = std::min(count_, buffer_size_);
        std::sort(buffer_, buffer_ + size);
        LOG(INFO) << header << ": count=" << count_ << ", "
                  << "p50=" << buffer_[percentile(size, 0.5)] << ", "
                  << "p70=" << buffer_[percentile(size, 0.7)] << ", "
                  << "p90=" << buffer_[percentile(size, 0.9)] << ", "
                  << "p99=" << buffer_[percentile(size, 0.99)] << ", "
                  << "p99.9=" << buffer_[percentile(size, 0.999)];
    }

private:
    size_t buffer_size_;
    T* buffer_;
    size_t count_;
    size_t pos_;

    size_t percentile(size_t size, double p) {
        size_t idx = gsl::narrow_cast<size_t>(size * p + 0.5);
        if (idx < 0) {
            return 0;
        } else if (idx >= size) {
            return size - 1;
        } else {
            return idx;
        }
    }

    DISALLOW_COPY_AND_ASSIGN(Samples);
};

}  // namespace bench_utils
}  // namespace faas
