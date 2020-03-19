#pragma once

#include "base/common.h"

namespace faas {
namespace watchdog {

enum class RunMode {
    INVALID = 0,
    // In Serializing mode, a new fprocess is created for each
    // function call. Input data is sent via stdin, and output
    // data is read from stdout.
    SERIALIZING = 1,
    // Create function worker processes which can be used repeatedly
    // for processing function calls.
    // In FIXED mode, watchdog creates a fixed number of function
    // workers (set by max_num_func_workers). Function calls will be
    // dispatched in a round-round across workers.
    FUNC_WORKER_FIXED = 2,
    // In ON_DEMAND mode, watchdog creates a small number of function
    // workers (set by min_num_func_workers), and will start new
    // workers based on load, while the maximum number is bound by
    // max_num_func_workers.
    FUNC_WORKER_ON_DEMAND = 3
};

}  // namespace watchdog
}  // namespace faas
