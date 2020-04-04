#pragma once

#include "base/common.h"

#include <absl/strings/numbers.h>

namespace faas {
namespace utils {

inline std::string_view GetEnvVariable(std::string_view name,
                                        std::string_view default_value = "") {
    char* value = getenv(std::string(name).c_str());
    return value != nullptr ? value : default_value;
}

template<class IntType>
IntType GetEnvVariableAsInt(std::string_view name,
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
