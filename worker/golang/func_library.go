package main

import (
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

func (fl *FuncLibrary) funcCall(ctx context.Context, methodId uint16, input []byte) ([]byte, error) {
	if strings.HasPrefix(fl.funcConfig.FuncName, "grpc:") {
		if int(methodId) >= len(fl.funcConfig.GrpcMethods) {
			service := strings.TrimPrefix(fl.funcConfig.FuncName, "grpc:")
			return nil, fmt.Errorf("gRPC service %s cannot process method_id %d", service, int(methodId))
		}
		method := fl.funcConfig.GrpcMethods[methodId]
		output, err := fl.handler.GrpcCall(ctx, method, input)
		if err != nil {
			log.Print("[WARN] gRPC call failed: ", err)
		}
		return output, err
	} else {
		if methodId != 0 {
			return nil, fmt.Errorf("method_id must be 0 for non-gRPC function")
		}
		output, err := fl.handler.Call(ctx, input)
		if err != nil {
			log.Print("[WARN] Function call failed: ", err)
		}
		return output, err
	}
}
