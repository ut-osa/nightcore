#pragma once

#include "base/common.h"

#include <math.h>

namespace faas {
namespace utils {

class ExpMovingAvg {
public:
    explicit ExpMovingAvg(double alpha = 0.001, size_t min_samples = 128)
        : alpha_(alpha), min_samples_(min_samples), avg_(0), n_samples_(0) {}
    ~ExpMovingAvg() {}

    template<class T>
    void AddSample(T sample) {
        if (n_samples_ < min_samples_) {
            avg_ += static_cast<double>(sample) / min_samples_;
        } else {
            avg_ += alpha_ * (static_cast<double>(sample) - avg_);
        }
        n_samples_++;
    }

    double GetValue() {
        if (n_samples_ < min_samples_) {
            return 0;
        } else {
            return avg_;
        }
    }

    void Reset() {
        n_samples_ = 0;
        avg_ = 0;
    }

private:
    double alpha_;
    size_t min_samples_;
    double avg_;
    size_t n_samples_;

    DISALLOW_COPY_AND_ASSIGN(ExpMovingAvg);
};

class ExpMovingAvgExt {
public:
    explicit ExpMovingAvgExt(double tau_ms = 0, double alpha = 0.001,
                             double p = 1.0, size_t min_samples = 128)
        : tau_us_(tau_ms * 1000), alpha_(alpha),
          p_(p), min_samples_(min_samples), avg_(0),
          n_samples_(0), last_timestamp_us_(-1) {}
    ~ExpMovingAvgExt() {}

    template<class T>
    void AddSample(int64_t timestamp_us, T sample) {
        if (sample < 0) {
            LOG(WARNING) << "ExpMovingAvgExt is supposed to handle non-negative sample values";
            return;
        }
        if (last_timestamp_us_ > 0 && timestamp_us <= last_timestamp_us_) {
            return;
        }
        if (n_samples_ < min_samples_) {
            if (p_ == 0) {
                avg_ += std::log(static_cast<double>(sample)) / min_samples_;
            } else {
                avg_ += std::pow(static_cast<double>(sample), p_) / min_samples_;
            }
        } else {
            double alpha = alpha_;
            if (tau_us_ > 0) {
                alpha = 1.0 - std::exp(-(timestamp_us - last_timestamp_us_) / tau_us_);
            }
            if (p_ == 0) {
                avg_ += alpha * (std::log(static_cast<double>(sample)) - avg_);
            } else {
                avg_ += alpha * (std::pow(static_cast<double>(sample), p_) - avg_);
            }
        }
        last_timestamp_us_ = timestamp_us;
        n_samples_++;
    }

    double GetValue() {
        if (n_samples_ < min_samples_) {
            return 0;
        } else {
            if (p_ == 0) {
                return std::exp(avg_);
            } else {
                return std::pow(avg_, 1.0 / p_);
            }
        }
    }

    void Reset() {
        n_samples_ = 0;
        avg_ = 0;
        last_timestamp_us_ = -1;
    }

private:
    double tau_us_;
    double alpha_;
    double p_;
    size_t min_samples_;
    double avg_;
    size_t n_samples_;
    int64_t last_timestamp_us_;

    DISALLOW_COPY_AND_ASSIGN(ExpMovingAvgExt);
};

}  // namespace utils
}  // namespace faas
