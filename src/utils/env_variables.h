#pragma once

#include "base/common.h"

#include <absl/strings/numbers.h>

namespace faas {
namespace utils {

inline absl::string_view GetEnvVariable(absl::string_view name,
                                        absl::string_view default_value = "") {
    char* value = getenv(std::string(name).c_str());
    return value != nullptr ? value : default_value;
}

template<class IntType>
IntType GetEnvVariableAsInt(absl::string_view name,
                            IntType default_value = 0) {
    char* value = getenv(std::string(name).c_str());
    if (value == nullptr) {
        return default_value;
    }
    IntType result;
    if (!absl::SimpleAtoi(value, &result)) {
        return default_value;
    }
    return result;
}

}  // namespace utils
}  // namespace faas
