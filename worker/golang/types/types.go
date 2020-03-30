package types

import (
	"context"
)

type Environment interface {
	InvokeFunc(ctx context.Context, funcName string, input []byte) ( /* output */ []byte, error)
}

// Function handler is compiled as a Go plugin.
// The worker wrapper will lookup the symbol `Handler` from the plugin,
// which is expected to implement FuncHandler interface.
type FuncHandler interface {
	Init(environment Environment) error
	Call(ctx context.Context, input []byte) ( /* output */ []byte, error)
}
