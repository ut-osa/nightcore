package main

import (
	"context"
	"cs.utexas.edu/zjia/faas/types"
	"plugin"
)

type FuncLibrary struct {
	p       *plugin.Plugin
	handler types.FuncHandler
}

func newFuncLibrary(libraryPath string) (*FuncLibrary, error) {
	p, err := plugin.Open(libraryPath)
	if err != nil {
		return nil, err
	}
	handler, err := p.Lookup("Handler")
	if err != nil {
		return nil, err
	}
	return &FuncLibrary{
		p:       p,
		handler: handler.(types.FuncHandler),
	}, nil
}

func (fl *FuncLibrary) init(environment types.Environment) error {
	return fl.handler.Init(environment)
}

func (fl *FuncLibrary) funcCall(ctx context.Context, input []byte) ([]byte, error) {
	return fl.handler.Call(ctx, input)
}
