package main

import (
	"fmt"
	"context"

	"cs.utexas.edu/zjia/faas"
	"cs.utexas.edu/zjia/faas/types"
)

type fooHandler struct {
	env types.Environment
}

type barHandler struct {
	env types.Environment
}

type funcHandlerFactory struct {
}

func (f *funcHandlerFactory) New(env types.Environment, funcName string) (types.FuncHandler, error) {
	if funcName == "Foo" {
		return &fooHandler{env: env}, nil
	} else if funcName == "Bar" {
		return &barHandler{env: env}, nil
	} else {
		return nil, nil
	}
}

func (f *funcHandlerFactory) GrpcNew(env types.Environment, service string) (types.GrpcFuncHandler, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (h *fooHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	barOutput, err := h.env.InvokeFunc(ctx, "Bar", input)
	if err != nil {
		return nil, err
	}
	output := fmt.Sprintf("From function Bar: %s", string(barOutput))
	return []byte(output), nil
}

func (h *barHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	output := fmt.Sprintf("%s, World\n", string(input))
	return []byte(output), nil
}

func main() {
	faas.Serve(&funcHandlerFactory{})
}
