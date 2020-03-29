package main

import (
	"encoding/binary"
)

// Status enum
const (
	Status_INVALID         uint16 = 0
	Status_OK              uint16 = 1
	Status_WATCHDOG_EXISTS uint16 = 2
)

// Role enum
const (
	Role_INVALID     uint16 = 0
	Role_WATCHDOG    uint16 = 1
	Role_FUNC_WORKER uint16 = 2
)

type HandshakeMessage struct {
	role   uint16
	funcId uint16
}

const HandshakeMessageByteSize = 2 /* role */ + 2 /* funcId */

func serializeHandshakeMessage(message HandshakeMessage) []byte {
	b := make([]byte, HandshakeMessageByteSize)
	binary.LittleEndian.PutUint16(b[0:], message.role)
	binary.LittleEndian.PutUint16(b[2:], message.funcId)
	return b
}

type HandshakeResponse struct {
	status   uint16
	clientId uint16
}

const HandshakeResponseByteSize = 2 /* status */ + 2 /* clientId */

func parseHandshakeResponse(data []byte) HandshakeResponse {
	return HandshakeResponse{
		status:   binary.LittleEndian.Uint16(data[0:]),
		clientId: binary.LittleEndian.Uint16(data[2:]),
	}
}

type FuncCall struct {
	funcId   uint16
	clientId uint16
	callId   uint32
}

const FuncCallByteSize = 8

func serializeFuncCall(funcCall FuncCall) []byte {
	b := make([]byte, FuncCallByteSize)
	binary.LittleEndian.PutUint16(b[0:], funcCall.funcId)
	binary.LittleEndian.PutUint16(b[2:], funcCall.clientId)
	binary.LittleEndian.PutUint32(b[4:], funcCall.callId)
	return b
}

func serializeFuncCallIntoBuffer(funcCall FuncCall, b []byte) {
	binary.LittleEndian.PutUint16(b[0:], funcCall.funcId)
	binary.LittleEndian.PutUint16(b[2:], funcCall.clientId)
	binary.LittleEndian.PutUint32(b[4:], funcCall.callId)
}

func parseFuncCall(data []byte) FuncCall {
	return FuncCall{
		funcId:   binary.LittleEndian.Uint16(data[0:]),
		clientId: binary.LittleEndian.Uint16(data[2:]),
		callId:   binary.LittleEndian.Uint32(data[4:]),
	}
}

func fullFuncCallId(funcCall FuncCall) uint64 {
	return (uint64(funcCall.callId) << 32) +
		(uint64(funcCall.clientId) << 16) +
		uint64(funcCall.funcId)
}

// MessageType enum
const (
	MessageType_INVALID            uint16 = 0
	MessageType_INVOKE_FUNC        uint16 = 1
	MessageType_FUNC_CALL_COMPLETE uint16 = 2
	MessageType_FUNC_CALL_FAILED   uint16 = 3
)

type Message struct {
	messageType uint16
	funcCall    FuncCall
}

const MessageByteSize = 2 /* messageType */ + FuncCallByteSize /* funcCall */

func serializeMessage(message Message) []byte {
	b := make([]byte, MessageByteSize)
	binary.LittleEndian.PutUint16(b[0:], message.messageType)
	serializeFuncCallIntoBuffer(message.funcCall, b[2:])
	return b
}

func parseMessage(data []byte) Message {
	return Message{
		messageType: binary.LittleEndian.Uint16(data[0:]),
		funcCall:    parseFuncCall(data[2:]),
	}
}
