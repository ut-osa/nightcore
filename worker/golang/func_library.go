package main

import (
	"bytes"
	"context"
	"cs.utexas.edu/zjia/faas/types"
	"fmt"
	"log"
	"plugin"
	"strings"
)

type FuncLibrary struct {
	p          *plugin.Plugin
	handler    types.FuncHandler
	funcConfig *FuncConfigEntry
}

func newFuncLibrary(libraryPath string, funcConfigEntry *FuncConfigEntry) (*FuncLibrary, error) {
	p, err := plugin.Open(libraryPath)
	if err != nil {
		return nil, err
	}
	handler, err := p.Lookup("Handler")
	if err != nil {
		return nil, err
	}
	return &FuncLibrary{
		p:          p,
		handler:    handler.(types.FuncHandler),
		funcConfig: funcConfigEntry,
	}, nil
}

func (fl *FuncLibrary) init(environment types.Environment) error {
	return fl.handler.Init(environment)
}

func (fl *FuncLibrary) funcCall(ctx context.Context, input []byte) ([]byte, error) {
	if strings.HasPrefix(fl.funcConfig.FuncName, "grpc:") {
		pos := bytes.IndexByte(input, '\x00')
		if pos == -1 {
			return nil, fmt.Errorf("Invalid input")
		}
		method := string(input[0:pos])
		if !fl.funcConfig.hasGrpcMethod(method) {
			service := strings.TrimPrefix(fl.funcConfig.FuncName, "grpc:")
			return nil, fmt.Errorf("gRPC service %s cannot process method %s", service, method)
		}
		output, err := fl.handler.GrpcCall(ctx, method, input[pos+1:])
		if err != nil {
			log.Print("[WARN] gRPC call failed: ", err)
		}
		return output, err
	} else {
		output, err := fl.handler.Call(ctx, input)
		if err != nil {
			log.Print("[WARN] Function call failed: ", err)
		}
		return output, err
	}
}
