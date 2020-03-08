#pragma once

#include "base/common.h"

namespace faas {
namespace watchdog {

enum class RunMode {
    INVALID = 0,
    // In Serializing mode, a new fprocess is created for each
    // function call. Input data is sent via stdin, and output
    // data is read from stdout.
    SERIALIZING = 1
};

}  // namespace watchdog
}  // namespace faas
