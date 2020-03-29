package main

import (
	"plugin"
)

type FuncLibrary struct {
	p          *plugin.Plugin
	initFn     plugin.Symbol
	funcCallFn plugin.Symbol
}

func newFuncLibrary(libraryPath string) (*FuncLibrary, error) {
	p, err := plugin.Open(libraryPath)
	if err != nil {
		return nil, err
	}
	initFn, err := p.Lookup("FaasInit")
	if err != nil {
		return nil, err
	}
	funcCallFn, err := p.Lookup("FaasFuncCall")
	if err != nil {
		return nil, err
	}
	return &FuncLibrary{
		p:          p,
		initFn:     initFn,
		funcCallFn: funcCallFn,
	}, nil
}

func (fl *FuncLibrary) init(invokeFuncFn func(string, []byte) ([]byte, error)) error {
	return fl.initFn.(func(func(string, []byte) ([]byte, error)) error)(invokeFuncFn)
}

func (fl *FuncLibrary) funcCall(input []byte) ([]byte, error) {
	return fl.funcCallFn.(func([]byte) ([]byte, error))(input)
}
