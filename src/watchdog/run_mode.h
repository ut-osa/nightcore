#pragma once

#include "base/common.h"

namespace faas {
namespace watchdog {

enum class RunMode {
    INVALID = 0,
    SERIALIZING = 1
};

}  // namespace watchdog
}  // namespace faas
