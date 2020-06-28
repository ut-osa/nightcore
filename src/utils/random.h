#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

// Thread-safe
float GetRandomFloat(float a = 0.0f, float b = 1.0f);  // In [a,b]

}  // namespace utils
}  // namespace faas
