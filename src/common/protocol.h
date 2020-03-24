#pragma once

#include <stdint.h>

namespace faas {
namespace protocol {

enum class Status {
    INVALID = 0,
    OK = 1,
    WATCHDOG_EXISTS = 2
};

enum class Role {
    INVALID = 0,
    WATCHDOG = 1,
    FUNC_WORKER = 2
};

struct HandshakeMessage {
    uint16_t role;
    uint16_t func_id;
} __attribute__((packed));

struct HandshakeResponse {
    uint16_t status;
    uint16_t client_id;
} __attribute__((packed));

union FuncCall {
    struct {
        uint16_t func_id;
        uint16_t client_id;
        uint32_t call_id;
    } __attribute__((packed));
    uint64_t full_call_id;
};
static_assert(sizeof(FuncCall) == 8, "Unexpected FuncCall size");

enum class MessageType {
    INVALID = 0,
    INVOKE_FUNC = 1,
    FUNC_CALL_COMPLETE = 2,
    FUNC_CALL_FAILED = 3
};

struct Message {
#ifdef __FAAS_ENABLE_PROFILING
    uint64_t send_timestamp;
    uint32_t processing_time;
#endif
    uint16_t message_type;
    FuncCall func_call;
} __attribute__((packed));

}  // namespace protocol
}  // namespace faas
