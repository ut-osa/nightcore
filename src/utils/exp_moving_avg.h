#pragma once

#include "base/common.h"

#include <math.h>

namespace faas {
namespace utils {

class ExpMovingAvg {
public:
    explicit ExpMovingAvg(double alpha = 0.999, double p = 1.0)
        : alpha_(alpha), p_(p), avg_(0), initial_(true) {}
    ~ExpMovingAvg() {}

    template<class T>
    void AddSample(T sample) {
        if (sample < 0) {
            LOG(WARNING) << "ExpMovingAvg is supposed to handle non-negative sample values";
            return;
        }
        if (initial_) {
            avg_ = std::pow(static_cast<double>(sample), p_);
            initial_ = false;
        } else {
            avg_ = alpha_ * avg_ + (1 - alpha_) * std::pow(static_cast<double>(sample), p_);
        }
    }

    double GetValue() {
        if (initial_) {
            return 0;
        } else {
            return std::pow(avg_, 1.0 / p_);
        }
    }

    void Reset() {
        initial_ = true;
        avg_ = 0;
    }

private:
    double alpha_;
    double p_;
    double avg_;
    bool initial_;

    DISALLOW_COPY_AND_ASSIGN(ExpMovingAvg);
};

}  // namespace utils
}  // namespace faas
