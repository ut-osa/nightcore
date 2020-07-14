#pragma once

#include "base/common.h"

#include <math.h>

namespace faas {
namespace utils {

class ExpMovingAvg {
public:
    explicit ExpMovingAvg(double alpha = 0.999, double p = 1.0, size_t min_samples = 128)
        : alpha_(alpha), p_(p), min_samples_(min_samples), avg_(0), n_samples_(0) {}
    ~ExpMovingAvg() {}

    template<class T>
    void AddSample(T sample) {
        if (sample < 0) {
            LOG(WARNING) << "ExpMovingAvg is supposed to handle non-negative sample values";
            return;
        }
        if (n_samples_ < min_samples_) {
            if (p_ == 0) {
                avg_ += std::log(static_cast<double>(sample)) / min_samples_;
            } else {
                avg_ += std::pow(static_cast<double>(sample), p_) / min_samples_;
            }
        } else {
            if (p_ == 0) {
                avg_ = alpha_ * avg_ + (1 - alpha_) * std::log(static_cast<double>(sample));
            } else {
                avg_ = alpha_ * avg_ + (1 - alpha_) * std::pow(static_cast<double>(sample), p_);
            }
        }
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
    }

private:
    double alpha_;
    double p_;
    size_t min_samples_;
    double avg_;
    size_t n_samples_;

    DISALLOW_COPY_AND_ASSIGN(ExpMovingAvg);
};

}  // namespace utils
}  // namespace faas
