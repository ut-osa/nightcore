#pragma once

#ifndef __FAAS_SRC
#error utils/perf_event.h cannot be included outside
#endif

#include "base/common.h"

#include <linux/perf_event.h>

namespace faas {
namespace utils {

class PerfEventGroup {
public:
    PerfEventGroup();
    ~PerfEventGroup();

    void set_cpu(int cpu) { cpu_ = cpu; }
    void set_exclude_user(bool value) { exclude_user_ = value; }
    void set_exclude_kernel(bool value) { exclude_kernel_ = value; }

    bool AddEvent(uint32_t type, uint64_t config);

    void Reset();
    void Enable();
    void Disable();
    std::vector<uint64_t> ReadValues(); 

    void ResetAndEnable() {
        Reset();
        Enable();
    }

private:
    int cpu_;
    bool exclude_user_;
    bool exclude_kernel_;

    int group_fd_;
    std::vector<int> event_fds_;

    DISALLOW_COPY_AND_ASSIGN(PerfEventGroup);
};

}  // namespace utils
}  // namespace faas
