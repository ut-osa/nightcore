#pragma once

#include "base/common.h"

#ifdef __FAAS_HAVE_ABSL
#include <absl/strings/numbers.h>
#endif

namespace faas {
namespace utils {

inline std::string_view GetEnvVariable(std::string_view name,
                                       std::string_view default_value = "") {
    char* value = getenv(std::string(name).c_str());
    return value != nullptr ? value : default_value;
}

#ifdef __FAAS_HAVE_ABSL

template<class IntType = int>
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

#else

inline int GetEnvVariableAsInt(std::string_view name, int default_value = 0) {
    char* value = getenv(std::string(name).c_str());
    if (value == nullptr) {
        return default_value;
    }
    return atoi(value);
}

#endif

}  // namespace utils
}  // namespace faas
