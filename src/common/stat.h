#pragma once

#include "base/common.h"
#include "common/time.h"

namespace faas {
namespace stat {

class ReportTimer {
public:
    static constexpr uint32_t kDefaultReportIntervalInMs = 10000;  /* 10 seconds */

    explicit ReportTimer(uint32_t report_interval_in_ms = kDefaultReportIntervalInMs)
        : report_interval_in_ms_(report_interval_in_ms), last_report_time_(-1) {}
    ~ReportTimer() {}

    void set_report_interval_in_ms(uint32_t value) {
        report_interval_in_ms_ = value;
    }

    bool Check() {
        int64_t current_time = GetMonotonicMicroTimestamp();
        if (last_report_time_ == -1) {
            last_report_time_ = current_time;
            return false;
        } else {
            return current_time - last_report_time_
                     > int64_t{report_interval_in_ms_} * 1000; 
        }
    }

    void MarkReport(int* duration_ms) {
        int64_t current_time = GetMonotonicMicroTimestamp();
        *duration_ms = gsl::narrow_cast<int>((current_time - last_report_time_) / 1000);
        last_report_time_ = current_time;
    }

private:
    uint32_t report_interval_in_ms_;
    int64_t last_report_time_;

    DISALLOW_COPY_AND_ASSIGN(ReportTimer);
};

template<class T>
class StatisticsCollector {
public:
    static constexpr size_t kDefaultMinReportSamples = 200;

    struct Report {
        T p50; T p70; T p90; T p99; T p99_9;
    };

    typedef std::function<void(int /* duration_ms */, size_t /* n_samples */,
                               const Report& /* report */)> ReportCallback;
    static ReportCallback StandardReportCallback(std::string_view stat_name) {
        std::string stat_name_copy = std::string(stat_name);
        return [stat_name_copy] (int duration_ms, size_t n_samples, const Report& report) {
            LOG(INFO) << stat_name_copy << " statistics (" << n_samples << " samples): "
                      << "p50=" << report.p50 << ", "
                      << "p70=" << report.p70 << ", "
                      << "p90=" << report.p90 << ", "
                      << "p99=" << report.p99 << ", "
                      << "p99.9=" << report.p99_9;
        };
    }

    explicit StatisticsCollector(ReportCallback report_callback)
        : min_report_samples_(kDefaultMinReportSamples),
          report_callback_(report_callback) {}

    ~StatisticsCollector() {}

    void set_report_interval_in_ms(uint32_t value) {
        absl::MutexLock lk(&mu_);
        report_timer_.set_report_interval_in_ms(value);
    }
    void set_min_report_samples(size_t value) {
        min_report_samples_ = value;
    }

    void AddSample(T sample) {
        absl::MutexLock lk(&mu_);
        samples_.push_back(sample);
        if (samples_.size() >= min_report_samples_ && report_timer_.Check()) {
            int duration_ms;
            Report report = BuildReport();
            size_t n_samples = samples_.size();
            samples_.clear();
            report_timer_.MarkReport(&duration_ms);
            report_callback_(duration_ms, n_samples, report);
        }
    }

private:
    size_t min_report_samples_;
    ReportCallback report_callback_;

    absl::Mutex mu_;
    ReportTimer report_timer_ ABSL_GUARDED_BY(mu_);
    std::vector<T> samples_ ABSL_GUARDED_BY(mu_);

    inline Report BuildReport() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
        std::sort(samples_.begin(), samples_.end());
        return {
            .p50 = percentile(0.5),
            .p70 = percentile(0.7),
            .p90 = percentile(0.9),
            .p99 = percentile(0.99),
            .p99_9 = percentile(0.999)
        };
    }

    inline T percentile(double p) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
        size_t idx = gsl::narrow_cast<size_t>(samples_.size() * p + 0.5);
        if (idx < 0) idx = 0;
        if (idx >= samples_.size()) {
            idx = samples_.size() - 1;
        }
        return samples_[idx];
    }

    DISALLOW_COPY_AND_ASSIGN(StatisticsCollector);
};

template<class T>
constexpr size_t StatisticsCollector<T>::kDefaultMinReportSamples;

class Counter {
public:
    typedef std::function<void(int /* duration_ms */, int64_t /* new_value */,
                               int64_t /* old_value */)> ReportCallback;
    static ReportCallback StandardReportCallback(std::string_view counter_name) {
        std::string counter_name_copy = std::string(counter_name);
        return [counter_name_copy] (int duration_ms, int64_t new_value, int64_t old_value) {
            double rate = gsl::narrow_cast<double>(new_value - old_value) / duration_ms * 1000;
            LOG(INFO) << counter_name_copy << " counter: value=" << new_value << ", "
                      << "rate=" << rate << " per sec";
        };
    }

    explicit Counter(ReportCallback report_callback)
        : report_callback_(report_callback),
          value_(0), last_report_value_(0) {}
    
    ~Counter() {}

    void set_report_interval_in_ms(uint32_t value) {
        absl::MutexLock lk(&mu_);
        report_timer_.set_report_interval_in_ms(value);
    }

    void Tick(int delta = 1) {
        DCHECK_GT(delta, 0);
        absl::MutexLock lk(&mu_);
        value_ += delta;
        if (value_ > last_report_value_ && report_timer_.Check()) {
            int duration_ms;
            report_timer_.MarkReport(&duration_ms);
            report_callback_(duration_ms, value_, last_report_value_);
            last_report_value_ = value_;
        }
    }

private:
    ReportCallback report_callback_;

    absl::Mutex mu_;
    ReportTimer report_timer_ ABSL_GUARDED_BY(mu_);
    int64_t value_ ABSL_GUARDED_BY(mu_);
    int64_t last_report_value_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(Counter);
};

class CategoryCounter {
public:
    typedef std::function<void(int /* duration_ms */,
                               const std::map<int, int64_t>& /* values */)> ReportCallback;
    static ReportCallback StandardReportCallback(std::string_view counter_name) {
        std::string counter_name_copy = std::string(counter_name);
        return [counter_name_copy] (int duration_ms, const std::map<int, int64_t>& values) {
            int64_t sum = 0;
            for (const auto& entry : values) {
                sum += entry.second;
            }
            std::ostringstream stream;
            bool first = true;
            for (const auto& entry : values) {
                if (entry.second == 0) continue;
                double percentage = gsl::narrow_cast<double>(entry.second) / sum * 100;
                if (!first) {
                    stream << ", ";
                } else {
                    first = false;
                }
                stream << entry.first << "=" << entry.second << "(" << percentage << "%)";
            }
            LOG(INFO) << counter_name_copy << " counter: " << stream.str();
        };
    }

    explicit CategoryCounter(ReportCallback report_callback)
        : report_callback_(report_callback),
          sum_(0) {}
    
    ~CategoryCounter() {}

    void Tick(int category, int delta = 1) {
        DCHECK_GT(delta, 0);
        absl::MutexLock lk(&mu_);
        values_[category] += delta;
        sum_ += delta;
        if (sum_ > 0 && report_timer_.Check()) {
            int duration_ms;
            report_timer_.MarkReport(&duration_ms);
            report_callback_(duration_ms, values_);
            for (auto& entry : values_) {
                entry.second = 0;
            }
            sum_ = 0;
        }
    }

private:
    ReportCallback report_callback_;

    absl::Mutex mu_;
    ReportTimer report_timer_ ABSL_GUARDED_BY(mu_);
    std::map<int, int64_t> values_ ABSL_GUARDED_BY(mu_);
    int64_t sum_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(CategoryCounter);
};

}  // namespace stat
}  // namespace faas
