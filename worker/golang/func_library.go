package main

import (
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

func (fl *FuncLibrary) init(invoker types.FuncInvoker) error {
	return fl.handler.Init(invoker)
}

func (fl *FuncLibrary) funcCall(input []byte) ([]byte, error) {
	return fl.handler.Call(input)
}
