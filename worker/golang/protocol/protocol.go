package protocol

import (
	"encoding/binary"
)

type FuncCall struct {
	FuncId   uint16
	MethodId uint16
	ClientId uint16
	CallId   uint32
}

const FuncCallByteSize = 8

const FuncIdBits = 8
const MethodIdBits = 6
const ClientIdBits = 14

func (funcCall *FuncCall) FullCallId() uint64 {
	return uint64(funcCall.FuncId) +
		(uint64(funcCall.MethodId) << FuncIdBits) +
		(uint64(funcCall.ClientId) << (FuncIdBits + MethodIdBits)) +
		(uint64(funcCall.CallId) << (FuncIdBits + MethodIdBits + ClientIdBits))
}

func FuncCallFromFullCallId(fullCallId uint64) FuncCall {
	return FuncCall{
		FuncId:   uint16(fullCallId & ((1 << FuncIdBits) - 1)),
		MethodId: uint16((fullCallId >> FuncIdBits) & ((1 << MethodIdBits) - 1)),
		ClientId: uint16((fullCallId >> (FuncIdBits + MethodIdBits)) & ((1 << ClientIdBits) - 1)),
		CallId:   uint32(fullCallId >> (FuncIdBits + MethodIdBits + ClientIdBits)),
	}
}

// MessageType enum
const (
	MessageType_INVALID               uint16 = 0
	MessageType_FUNC_WORKER_HANDSHAKE uint16 = 3
	MessageType_HANDSHAKE_RESPONSE    uint16 = 4
	MessageType_CREATE_FUNC_WORKER    uint16 = 5
	MessageType_INVOKE_FUNC           uint16 = 6
	MessageType_DISPATCH_FUNC_CALL    uint16 = 7
	MessageType_FUNC_CALL_COMPLETE    uint16 = 8
	MessageType_FUNC_CALL_FAILED      uint16 = 9
)

const MessageTypeBits = 4

// Matches __FAAS_CACHE_LINE_SIZE in base/macro.h
const MessageHeaderByteSize = 64

// Matches __FAAS_MESSAGE_SIZE in base/macro.h
const MessageFullByteSize = 1024
const MessageInlineDataSize = MessageFullByteSize - MessageHeaderByteSize

const (
	FLAG_FuncWorkerUseEngineSocket uint32 = 1
	FLAG_UseFifoForNestedCall      uint32 = 2
)

func GetFlagsFromMessage(buffer []byte) uint32 {
	return binary.LittleEndian.Uint32(buffer[28:32])
}

func GetFuncCallFromMessage(buffer []byte) FuncCall {
	tmp := binary.LittleEndian.Uint64(buffer[0:8])
	return FuncCallFromFullCallId(tmp >> MessageTypeBits)
}

func getMessageType(buffer []byte) uint16 {
	firstByte := buffer[0]
	return uint16(firstByte & ((1 << MessageTypeBits) - 1))
}

func IsHandshakeResponseMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_HANDSHAKE_RESPONSE
}

func IsCreateFuncWorkerMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_CREATE_FUNC_WORKER
}

func IsDispatchFuncCallMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_DISPATCH_FUNC_CALL
}

func IsFuncCallCompleteMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_FUNC_CALL_COMPLETE
}

func IsFuncCallFailedMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_FUNC_CALL_FAILED
}

func NewEmptyMessage() []byte {
	return make([]byte, MessageFullByteSize)
}

func NewFuncWorkerHandshakeMessage(funcId uint16, clientId uint16) []byte {
	buffer := NewEmptyMessage()
	tmp := uint64(funcId) << MessageTypeBits
	tmp += uint64(clientId) << (MessageTypeBits + FuncIdBits + MethodIdBits)
	tmp += uint64(MessageType_FUNC_WORKER_HANDSHAKE)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	return buffer
}

func NewInvokeFuncCallMessage(funcCall FuncCall, parentCallId uint64) []byte {
	buffer := NewEmptyMessage()
	tmp := (funcCall.FullCallId() << MessageTypeBits) + uint64(MessageType_INVOKE_FUNC)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	binary.LittleEndian.PutUint64(buffer[8:16], parentCallId)
	return buffer
}

func NewFuncCallCompleteMessage(funcCall FuncCall, processingTime int32) []byte {
	buffer := NewEmptyMessage()
	tmp := (funcCall.FullCallId() << MessageTypeBits) + uint64(MessageType_FUNC_CALL_COMPLETE)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	binary.LittleEndian.PutUint32(buffer[12:16], uint32(processingTime))
	return buffer
}

func NewFuncCallFailedMessage(funcCall FuncCall) []byte {
	buffer := NewEmptyMessage()
	tmp := (funcCall.FullCallId() << MessageTypeBits) + uint64(MessageType_FUNC_CALL_FAILED)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	return buffer
}

func GetClientIdFromMessage(buffer []byte) uint16 {
	return GetFuncCallFromMessage(buffer).ClientId
}

func GetSendTimestampFromMessage(buffer []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buffer[16:24]))
}

func SetSendTimestampInMessage(buffer []byte, sendTimestamp int64) {
	binary.LittleEndian.PutUint64(buffer[16:24], uint64(sendTimestamp))
}

func GetPayloadSizeFromMessage(buffer []byte) int32 {
	return int32(binary.LittleEndian.Uint32(buffer[24:28]))
}

func SetPayloadSizeInMessage(buffer []byte, payloadSize int32) {
	binary.LittleEndian.PutUint32(buffer[24:28], uint32(payloadSize))
}

func FillInlineDataInMessage(buffer []byte, data []byte) {
	n := copy(buffer[MessageHeaderByteSize:], data)
	SetPayloadSizeInMessage(buffer, int32(n))
}

func GetInlineDataFromMessage(buffer []byte) []byte {
	payloadSize := GetPayloadSizeFromMessage(buffer)
	if payloadSize > 0 {
		return buffer[MessageHeaderByteSize : MessageHeaderByteSize+payloadSize]
	} else {
		return nil
	}
}

func SetDispatchDelayInMessage(buffer []byte, dispatchDelay int32) {
	binary.LittleEndian.PutUint32(buffer[8:12], uint32(dispatchDelay))
}
