#pragma once

#include "base/common.h"
#include "common/time.h"

namespace faas {
namespace protocol {

constexpr int kFuncIdBits   = 8;
constexpr int kMethodIdBits = 6;
constexpr int kClientIdBits = 14;

constexpr int kMaxFuncId   = (1 << kFuncIdBits) - 1;
constexpr int kMaxMethodId = (1 << kMethodIdBits) - 1;
constexpr int kMaxClientId = (1 << kClientIdBits) - 1;

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

constexpr FuncCall kInvalidFuncCall = { .full_call_id = 0 };

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

inline std::string FuncCallDebugString(const FuncCall& func_call) {
    if (func_call.method_id == 0) {
        return fmt::format("func_id={}, client_id={}, call_id={}",
                           func_call.func_id, func_call.client_id, func_call.call_id);
    } else {
        return fmt::format("func_id={}, method_id={}, client_id={}, call_id={}",
                           func_call.func_id, func_call.method_id,
                           func_call.client_id, func_call.call_id);
    }
}

#undef NEW_EMPTY_FUNC_CALL

enum class MessageType : uint16_t {
    INVALID               = 0,
    LAUNCHER_HANDSHAKE    = 1,
    FUNC_WORKER_HANDSHAKE = 2,
    HANDSHAKE_RESPONSE    = 3,
    CREATE_FUNC_WORKER    = 4,
    INVOKE_FUNC           = 5,
    DISPATCH_FUNC_CALL    = 6,
    FUNC_CALL_COMPLETE    = 7,
    FUNC_CALL_FAILED      = 8
};

struct Message {
    struct {
        uint16_t message_type : 4;
        uint16_t func_id      : 8;
        uint16_t method_id    : 6;
        uint16_t client_id    : 14;
        uint32_t call_id;
    }  __attribute__ ((packed));
    union {
        uint64_t parent_call_id;  // Used in INVOKE_FUNC, saved as full_call_id
        int32_t processing_time;  // Used in FUNC_CALL_COMPLETE
    };
    int64_t send_timestamp;
    int32_t payload_size;  // Used in HANDSHAKE_RESPONSE, INVOKE_FUNC, FUNC_CALL_COMPLETE

    char padding[__FAAS_CACHE_LINE_SIZE - 28];
    char inline_data[__FAAS_MESSAGE_SIZE - __FAAS_CACHE_LINE_SIZE]
        __attribute__ ((aligned (__FAAS_CACHE_LINE_SIZE)));
};

#define MESSAGE_INLINE_DATA_SIZE (__FAAS_MESSAGE_SIZE - __FAAS_CACHE_LINE_SIZE)
static_assert(sizeof(Message) == __FAAS_MESSAGE_SIZE, "Unexpected Message size");

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

inline bool IsDispatchFuncCallMessage(const Message& message) {
    return static_cast<MessageType>(message.message_type) == MessageType::DISPATCH_FUNC_CALL;
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
             || IsDispatchFuncCallMessage(message)
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

inline void SetInlineDataInMessage(Message* message, std::span<const char> data) {
    message->payload_size = gsl::narrow_cast<int32_t>(data.size());
    DCHECK(data.size() <= MESSAGE_INLINE_DATA_SIZE);
    if (data.size() > 0) {
        memcpy(message->inline_data, data.data(), data.size());
    }
}

inline std::span<const char> GetInlineDataFromMessage(const Message& message) {
    if (IsInvokeFuncMessage(message)
          || IsDispatchFuncCallMessage(message)
          || IsFuncCallCompleteMessage(message)
          || IsLauncherHandshakeMessage(message)) {
        if (message.payload_size > 0) {
            return std::span<const char>(
                message.inline_data, gsl::narrow_cast<size_t>(message.payload_size));
        }
    }
    return std::span<const char>();
}

inline int32_t ComputeMessageDelay(const Message& message) {
    if (message.send_timestamp > 0) {
        return gsl::narrow_cast<int32_t>(GetMonotonicMicroTimestamp() - message.send_timestamp);
    } else {
        return -1;
    }
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

inline Message NewInvokeFuncMessage(const FuncCall& func_call, uint64_t parent_call_id) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::INVOKE_FUNC);
    SetFuncCallInMessage(&message, func_call);
    message.parent_call_id = parent_call_id;
    return message;
}

inline Message NewDispatchFuncCallMessage(const FuncCall& func_call) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::DISPATCH_FUNC_CALL);
    SetFuncCallInMessage(&message, func_call);
    return message;
}

inline Message NewFuncCallCompleteMessage(const FuncCall& func_call, int32_t processing_time) {
    NEW_EMPTY_MESSAGE(message);
    message.message_type = static_cast<uint16_t>(MessageType::FUNC_CALL_COMPLETE);
    SetFuncCallInMessage(&message, func_call);
    message.processing_time = processing_time;
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
