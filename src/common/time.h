#pragma once

#include "base/common.h"

#include <time.h>

namespace faas {

inline uint64_t GetMonotonicMicroTimestamp() {
    struct timespec tp;
    PCHECK(clock_gettime(CLOCK_MONOTONIC, &tp) == 0) << "clock_gettime failed";
    uint64_t ret = 0;
    ret += static_cast<uint64_t>(tp.tv_sec) * 1000000;
    ret += static_cast<uint64_t>(tp.tv_nsec) / 1000;
    return ret;
}

inline uint64_t GetRealtimeMicroTimestamp() {
    struct timespec tp;
    PCHECK(clock_gettime(CLOCK_REALTIME, &tp) == 0) << "clock_gettime failed";
    uint64_t ret = 0;
    ret += static_cast<uint64_t>(tp.tv_sec) * 1000000;
    ret += static_cast<uint64_t>(tp.tv_nsec) / 1000;
    return ret;
}

}  // namespace faas
