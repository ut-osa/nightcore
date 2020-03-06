#pragma once

#include <stdint.h>

namespace faas {
namespace protocol {

constexpr size_t kMaxFuncNameLength = 31;

enum class Status {
    INVALID = 0,
    OK = 1
};

struct WatchdogHandshakeMessage {
    char func_name[kMaxFuncNameLength+1];
    uint16_t func_id;
} __attribute__((packed));

struct WatchdogHandshakeResponse {
    uint16_t status;
    uint16_t caller_id;
} __attribute__((packed));

union FuncCall {
    struct {
        uint16_t func_id   : 2;
        uint16_t caller_id : 2;
        uint32_t call_id   : 4;
    };
    uint64_t full_call_id;
};
static_assert(sizeof(FuncCall) == 8, "Unexpected FuncCall size");

enum class MessageType {
    INVALID = 0,
    INVOKE_FUNC = 1,
    FUNC_CALL_COMPLETE = 2
};

struct Message {
    uint16_t message_type;
    FuncCall func_call;
} __attribute__((packed));

}  // namespace protocol
}  // namespace faas
