package types

import (
	"context"
)

type Environment interface {
	InvokeFunc(ctx context.Context, funcName string, input []byte) ( /* output */ []byte, error)
	GrpcCall(ctx context.Context, service string, method string, request []byte) ( /* reply */ []byte, error)
}

type FuncHandler interface {
	Init(environment Environment) error
	Call(ctx context.Context, input []byte) ( /* output */ []byte, error)
	GrpcCall(ctx context.Context, method string, request []byte) ( /* reply */ []byte, error)
}
