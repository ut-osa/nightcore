#pragma once

#include "base/common.h"
#include "common/time.h"
#include "utils/bst.h"
#include "utils/env_variables.h"
#include "utils/random.h"

#include <math.h>

namespace faas {
namespace stat {

class ReportTimer {
public:
    static constexpr uint32_t kDefaultReportIntervalInMs = 10000;  /* 10 seconds */

    explicit ReportTimer(uint32_t report_interval_in_ms = kDefaultReportIntervalInMs) {
        set_report_interval_in_ms(report_interval_in_ms);
        last_report_time_ = -1;
    }
    ~ReportTimer() {}

    void set_report_interval_in_ms(uint32_t value) {
        report_interval_in_ms_ = gsl::narrow_cast<uint32_t>(value * utils::GetRandomFloat(0.9, 1.1));
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

#ifdef __FAAS_USE_NEW_STAT_COLLECTOR

template<class T>
class StatisticsCollector {
public:
    static constexpr size_t kDefaultMinReportSamples = 200;

    struct Report {
        std::vector<std::pair</* p */ double, /* p-norm */ double>> data;
    };

    typedef std::function<void(int /* duration_ms */, size_t /* n_samples */,
                               const Report& /* report */)> ReportCallback;
    static ReportCallback StandardReportCallback(std::string_view stat_name) {
        std::string stat_name_copy = std::string(stat_name);
        return [stat_name_copy] (int duration_ms, size_t n_samples, const Report& report) {
            std::stringstream stream;
            stream << stat_name_copy << " statistics (" << n_samples << " samples): ";
            bool first = true;
            for (const auto& entry : report.data) {
                double p = entry.first;
                double pnorm = entry.second;
                if (first) {
                    first = false;
                } else {
                    stream << ", ";
                }
                stream << p << "-norm=" << pnorm;
            }
            LOG(INFO) << stream.str();
        };
    }

    explicit StatisticsCollector(ReportCallback report_callback)
        : min_report_samples_(kDefaultMinReportSamples),
          report_callback_(report_callback) {
        std::string pnorms(utils::GetEnvVariable("FAAS_STAT_PNORMS", "1,2,5,10"));
        size_t start = 0;
        while (true) {
            size_t end = pnorms.find(",", start);
            if (end == std::string::npos) {
                p_.push_back(atof(pnorms.substr(start).c_str()));
                break;
            } else {
                p_.push_back(atof(pnorms.substr(start, end - start).c_str()));
                start = end + 1;
            }
        }
        psum_.assign(p_.size(), 0.0);
    }

    ~StatisticsCollector() {}

    void set_report_interval_in_ms(uint32_t value) {
        report_timer_.set_report_interval_in_ms(value);
    }
    void set_min_report_samples(size_t value) {
        min_report_samples_ = value;
    }

    void AddSample(T sample) {
#ifndef __FAAS_DISABLE_STAT
        for (size_t i = 0; i < p_.size(); i++) {
            psum_[i] += std::pow(static_cast<double>(sample), p_[i]);
        }
        n_samples_++;
        if (n_samples_ >= min_report_samples_ && report_timer_.Check()) {
            int duration_ms;
            Report report = BuildReport();
            size_t n_samples = n_samples_;
            n_samples_ = 0;
            for (size_t i = 0; i < p_.size(); i++) {
                psum_[i] = 0;
            }
            report_timer_.MarkReport(&duration_ms);
            report_callback_(duration_ms, n_samples, report);
        }
#endif
    }

private:
    size_t min_report_samples_;
    ReportCallback report_callback_;

    ReportTimer report_timer_;
    size_t n_samples_;
    std::vector<double> p_;
    std::vector<double> psum_;

    inline Report BuildReport() {
        std::vector<std::pair<double, double>> data;
        data.resize(p_.size());
        for (size_t i = 0; i < p_.size(); i++) {
            data[i].first = p_[i];
            data[i].second = std::pow(psum_[i] / gsl::narrow_cast<double>(n_samples_), 1.0 / p_[i]);
        }
        return { .data = std::move(data) };
    }

    DISALLOW_COPY_AND_ASSIGN(StatisticsCollector);
};

#else  // __FAAS_USE_NEW_STAT_COLLECTOR

template<class T>
class StatisticsCollector {
public:
    static constexpr size_t kDefaultMinReportSamples = 200;

    struct Report {
        T p30; T p50; T p70; T p90; T p99; T p99_9;
    };

    typedef std::function<void(int /* duration_ms */, size_t /* n_samples */,
                               const Report& /* report */)> ReportCallback;
    static ReportCallback StandardReportCallback(std::string_view stat_name) {
        std::string stat_name_copy = std::string(stat_name);
        return [stat_name_copy] (int duration_ms, size_t n_samples, const Report& report) {
            LOG(INFO) << stat_name_copy << " statistics (" << n_samples << " samples): "
                      << "p30=" << report.p30 << ", "
                      << "p50=" << report.p50 << ", "
                      << "p70=" << report.p70 << ", "
                      << "p90=" << report.p90 << ", "
                      << "p99=" << report.p99 << ", "
                      << "p99.9=" << report.p99_9;
        };
    }

    explicit StatisticsCollector(ReportCallback report_callback)
        : min_report_samples_(kDefaultMinReportSamples),
          report_callback_(report_callback),
          force_enabled_(false) {}

    ~StatisticsCollector() {}

    void set_report_interval_in_ms(uint32_t value) {
        report_timer_.set_report_interval_in_ms(value);
    }
    void set_min_report_samples(size_t value) {
        min_report_samples_ = value;
    }
    void set_force_enabled(bool value) {
        force_enabled_ = value;
    }

    void AddSample(T sample) {
#ifdef __FAAS_DISABLE_STAT
        if (!force_enabled_) {
            return;
        }
#endif
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

    bool force_enabled_;
    ReportTimer report_timer_;
    std::vector<T> samples_;

    inline Report BuildReport() {
        std::sort(samples_.begin(), samples_.end());
        return {
            .p30 = percentile(0.3),
            .p50 = percentile(0.5),
            .p70 = percentile(0.7),
            .p90 = percentile(0.9),
            .p99 = percentile(0.99),
            .p99_9 = percentile(0.999)
        };
    }

    inline T percentile(double p) {
        size_t idx = gsl::narrow_cast<size_t>(samples_.size() * p + 0.5);
        if (idx < 0) idx = 0;
        if (idx >= samples_.size()) {
            idx = samples_.size() - 1;
        }
        return samples_[idx];
    }

    DISALLOW_COPY_AND_ASSIGN(StatisticsCollector);
};

#endif  // __FAAS_USE_NEW_STAT_COLLECTOR

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
        report_timer_.set_report_interval_in_ms(value);
    }

    void Tick(int delta = 1) {
#ifndef __FAAS_DISABLE_STAT
        DCHECK_GT(delta, 0);
        value_ += delta;
        if (value_ > last_report_value_ && report_timer_.Check()) {
            int duration_ms;
            report_timer_.MarkReport(&duration_ms);
            report_callback_(duration_ms, value_, last_report_value_);
            last_report_value_ = value_;
        }
#endif
    }

private:
    ReportCallback report_callback_;

    ReportTimer report_timer_;
    int64_t value_;
    int64_t last_report_value_;

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
#ifndef __FAAS_DISABLE_STAT
        DCHECK_GT(delta, 0);
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
#endif
    }

private:
    ReportCallback report_callback_;

    ReportTimer report_timer_;
    std::map<int, int64_t> values_;
    int64_t sum_;

    DISALLOW_COPY_AND_ASSIGN(CategoryCounter);
};

}  // namespace stat
}  // namespace faas
