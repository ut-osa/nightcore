#pragma once

#include "base/common.h"

#include <absl/time/time.h>
#include <absl/time/clock.h>
#include <absl/algorithm/container.h>

namespace faas {
namespace stat {

template<class T>
class StatisticsCollector {
public:
    static constexpr absl::Duration kDefaultReportInterval = absl::Seconds(10);
    static constexpr size_t kDefaultMinReportSamples = 200;

    struct Report {
        T p50; T p70; T p90; T p99; T p99_9;
    };

    typedef std::function<void(absl::Duration /* duration */, size_t /* n_samples */,
                               const Report& /* report */)> ReportCallback;
    static ReportCallback StandardReportCallback(absl::string_view stat_name) {
        std::string stat_name_copy = std::string(stat_name);
        return [stat_name_copy] (absl::Duration duration, size_t n_samples, const Report& report) {
            LOG(INFO) << stat_name_copy << " statistics (" << n_samples << " samples): "
                      << "p50=" << report.p50 << ", "
                      << "p70=" << report.p70 << ", "
                      << "p90=" << report.p90 << ", "
                      << "p99=" << report.p99 << ", "
                      << "p99.9=" << report.p99_9;
        };
    }

    explicit StatisticsCollector(ReportCallback report_callback)
        : report_interval_(kDefaultReportInterval),
          min_report_samples_(kDefaultMinReportSamples),
          report_callback_(report_callback),
          last_report_time_(absl::InfinitePast()) {}

    ~StatisticsCollector() {}

    void set_report_interval(absl::Duration interval) {
        report_interval_ = interval;
    }
    void set_min_report_samples(size_t value) {
        min_report_samples_ = value;
    }

    void AddSample(T sample) {
        absl::MutexLock lk(&mu_);
        if (last_report_time_ == absl::InfinitePast()) {
            last_report_time_ = absl::Now();
        }
        samples_.push_back(sample);
        absl::Time current_time = absl::Now();
        if (samples_.size() >= min_report_samples_
              && current_time >= last_report_time_ + report_interval_) {
            Report report = BuildReport();
            size_t n_samples = samples_.size();
            samples_.clear();
            report_callback_(current_time - last_report_time_, n_samples, report);
            last_report_time_ = current_time;
        }
    }

private:
    absl::Duration report_interval_;
    size_t min_report_samples_;
    ReportCallback report_callback_;

    absl::Mutex mu_;
    absl::Time last_report_time_ ABSL_GUARDED_BY(mu_);
    absl::InlinedVector<T, 1024> samples_ ABSL_GUARDED_BY(mu_);

    inline Report BuildReport() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
        absl::c_sort(samples_);
        return {
            .p50 = percentile(0.5),
            .p70 = percentile(0.7),
            .p90 = percentile(0.9),
            .p99 = percentile(0.99),
            .p99_9 = percentile(0.999)
        };
    }

    inline T percentile(double p) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
        size_t idx = static_cast<size_t>(samples_.size() * p + 0.5);
        if (idx < 0) idx = 0;
        if (idx >= samples_.size()) {
            idx = samples_.size() - 1;
        }
        return samples_[idx];
    }

    DISALLOW_COPY_AND_ASSIGN(StatisticsCollector);
};

template<class T>
constexpr absl::Duration StatisticsCollector<T>::kDefaultReportInterval;

template<class T>
constexpr size_t StatisticsCollector<T>::kDefaultMinReportSamples;

class Counter {
public:
    static constexpr absl::Duration kDefaultReportInterval = absl::Seconds(1);

    typedef std::function<void(absl::Duration /* duration */, uint64_t /* new_value */,
                               uint64_t /* old_value */)> ReportCallback;
    static ReportCallback StandardReportCallback(absl::string_view counter_name) {
        std::string counter_name_copy = std::string(counter_name);
        return [counter_name_copy] (absl::Duration duration, uint64_t new_value, uint64_t old_value) {
            double rate = static_cast<double>(new_value - old_value) / absl::ToDoubleSeconds(duration);
            LOG(INFO) << counter_name_copy << " counter: value=" << new_value << ", "
                      << "rate=" << rate << " per sec";
        };
    }

    explicit Counter(ReportCallback report_callback)
        : report_interval_(kDefaultReportInterval),
          report_callback_(report_callback),
          last_report_time_(absl::InfinitePast()),
          value_(0), last_report_value_(0) {}

    void Tick(uint32_t delta = 1) {
        absl::MutexLock lk(&mu_);
        if (last_report_time_ == absl::InfinitePast()) {
            last_report_time_ = absl::Now();
        }
        value_ += delta;
        absl::Time current_time = absl::Now();
        if (value_ > last_report_value_
              && current_time >= last_report_time_ + report_interval_) {
            report_callback_(current_time - last_report_time_, value_, last_report_value_);
            last_report_time_ = current_time;
            last_report_value_ = value_;
        }
    }

private:
    absl::Duration report_interval_;
    ReportCallback report_callback_;

    absl::Mutex mu_;
    absl::Time last_report_time_ ABSL_GUARDED_BY(mu_);
    uint64_t value_ ABSL_GUARDED_BY(mu_);
    uint64_t last_report_value_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(Counter);
};

}  // namespace stat
}  // namespace faas
