#pragma once

#include <stdint.h>

namespace faas {
namespace protocol {

constexpr size_t kMaxFunctionNameLength = 31;

enum Status {
    INVALID_STATUS = 0,
    OK_STATUS = 1
};

struct WatchdogHandshakeMessage {
    char function_name[kMaxFunctionNameLength+1];
} __attribute__((packed));

struct WatchdogHandshakeResponse {
    uint16_t status;
} __attribute__((packed));

}  // namespace protocol
}  // namespace faas
