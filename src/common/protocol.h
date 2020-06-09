#pragma once

#include "base/common.h"

namespace faas {
namespace protocol {

constexpr int kFuncIdBits   = 8;
constexpr int kMethodIdBits = 6;
constexpr int kClientIdBits = 14;

union FuncCall {
    struct {
        uint16_t func_id   : 8;
        uint16_t method_id : 6;
        uint16_t client_id : 14;
        uint32_t call_id   : 32;
        uint16_t padding   : 4;
    } __attribute__((packed));
    uint64_t full_call_id;
};
static_assert(sizeof(FuncCall) == 8, "Unexpected FuncCall size");

#define NEW_EMPTY_FUNC_CALL(var)      \
    FuncCall var;                     \
    memset(&var, 0, sizeof(FuncCall))

inline FuncCall NewFuncCall(uint16_t func_id, uint16_t client_id, uint32_t call_id) {
    NEW_EMPTY_FUNC_CALL(func_call);
    func_call.func_id = func_id;
    func_call.client_id = client_id;
    func_call.call_id = call_id;
    return func_call; 
}

inline FuncCall NewFuncCallWithMethod(uint16_t func_id, uint16_t method_id,
                                      uint16_t client_id, uint32_t call_id) {
    NEW_EMPTY_FUNC_CALL(func_call);
    func_call.func_id = func_id;
    func_call.method_id = method_id;
    func_call.client_id = client_id;
    func_call.call_id = call_id;
    return func_call; 
}

#undef NEW_EMPTY_FUNC_CALL

enum class MessageType : uint16_t {
    INVALID               = 0,
    LAUNCHER_HANDSHAKE    = 1,
    FUNC_WORKER_HANDSHAKE = 2,
    HANDSHAKE_RESPONSE    = 3,
    CREATE_FUNC_WORKER    = 4,
    INVOKE_FUNC           = 5,
    FUNC_CALL_COMPLETE    = 6,
    FUNC_CALL_FAILED      = 7
};

struct Message {
#ifdef __FAAS_ENABLE_PROFILING
    int64_t send_timestamp;
    int32_t processing_time;
#endif
    uint16_t message_type : 4;
    uint16_t func_id      : 8;
    uint16_t method_id    : 6;
    uint16_t client_id    : 14;
    union {
        uint32_t call_id;
        uint32_t payload_size;
    };
} __attribute__((packed));

#ifdef __FAAS_ENABLE_PROFILING
static_assert(sizeof(Message) == 20, "Unexpected Message size");
#else
static_assert(sizeof(Message) == 8, "Unexpected Message size");
#endif

inline bool IsInvalidMessage(const Message& message) {
    return static_cast<MessageType>(message.message_type) == MessageType::INVALID;
}

inline bool IsLauncherHandshakeMessage(const Message& message) {
    return static_cast<MessageType>(message.message_type) == MessageType::LAUNCHER_HANDSHAKE;
}

inline bool IsFuncWorkerHandshakeMessage(const Message& message) {
    return static_cast<MessageType>(message.message_type) == MessageType::FUNC_WORKER_HANDSHAKE;
}

inline bool IsHandshakeResponseMessage(const Message& message) {
    return static_cast<MessageType>(message.message_type) == MessageType::HANDSHAKE_RESPONSE;
}

inline bool IsCreateFuncWorkerMessage(const Message& message) {
    return static_cast<MessageType>(message.message_type) == MessageType::CREATE_FUNC_WORKER;
}

inline bool IsInvokeFuncMessage(const Message& message) {
    return static_cast<MessageType>(message.message_type) == MessageType::INVOKE_FUNC;
}

inline bool IsFuncCallCompleteMessage(const Message& message) {
    return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_COMPLETE;
}

inline bool IsFuncCallFailedMessage(const Message& message) {
    return static_cast<MessageType>(message.message_type) == MessageType::FUNC_CALL_FAILED;
}

inline void SetFuncCallInMessage(Message* message, const FuncCall& func_call) {
    message->func_id = func_call.func_id;
    message->method_id = func_call.method_id;
    message->client_id = func_call.client_id;
    message->call_id = func_call.call_id;
}

inline FuncCall GetFuncCallFromMessage(const Message& message) {
    DCHECK(IsInvokeFuncMessage(message)
             || IsFuncCallCompleteMessage(message)
             || IsFuncCallFailedMessage(message));
    FuncCall func_call;
    func_call.func_id = message.func_id;
    func_call.method_id = message.method_id;
    func_call.client_id = message.client_id;
    func_call.call_id = message.call_id;
    func_call.padding = 0;
    return func_call;
}

#define NEW_EMPTY_MESSAGE(var)       \
    Message var;                     \
    memset(&var, 0, sizeof(Message))

inline Message NewLauncherHandshakeMessage(uint16_t func_id) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::LAUNCHER_HANDSHAKE);
    message.func_id = func_id;
    return message;
}

inline Message NewFuncWorkerHandshakeMessage(uint16_t func_id, uint16_t client_id) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::FUNC_WORKER_HANDSHAKE);
    message.func_id = func_id;
    message.client_id = client_id;
    return message;
}

inline Message NewHandshakeResponseMessage(uint32_t payload_size) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::HANDSHAKE_RESPONSE);
    message.payload_size = payload_size;
    return message;
}

inline Message NewCreateFuncWorkerMessage(uint16_t client_id) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::CREATE_FUNC_WORKER);
    message.client_id = client_id;
    return message;
}

inline Message NewInvokeFuncMessage(const FuncCall& func_call) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC);
    SetFuncCallInMessage(&message, func_call);
    return message;
}

inline Message NewFuncCallCompleteMessage(const FuncCall& func_call) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
    SetFuncCallInMessage(&message, func_call);
    return message;
}

inline Message NewFuncCallFailedMessage(const FuncCall& func_call) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_FAILED);
    SetFuncCallInMessage(&message, func_call);
    return message;
}

#undef NEW_EMPTY_MESSAGE

}  // namespace protocol
}  // namespace faas
