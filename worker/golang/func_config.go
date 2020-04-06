package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

type FuncConfigEntry struct {
	FuncName    string   `json:"funcName"`
	FuncId      uint16   `json:"funcId"`
	GrpcMethods []string `json:"grpcMethods"`
}

type FuncConfig struct {
	entries           []*FuncConfigEntry
	entriesByFuncId   map[uint16]*FuncConfigEntry
	entriesByFuncName map[string]*FuncConfigEntry
}

func newFuncConfig(jsonPath string) (*FuncConfig, error) {
	jsonContents, err := ioutil.ReadFile(jsonPath)
	if err != nil {
		return nil, fmt.Errorf("Failed to read from file %s: %v", jsonPath, err)
	}
	fc := new(FuncConfig)
	err = json.Unmarshal(jsonContents, &fc.entries)
	if err != nil {
		return nil, fmt.Errorf("Failed to unmarshal json: %v", err)
	}
	fc.entriesByFuncId = make(map[uint16]*FuncConfigEntry)
	fc.entriesByFuncName = make(map[string]*FuncConfigEntry)
	for _, entry := range fc.entries {
		if fc.entriesByFuncId[entry.FuncId] != nil {
			return nil, fmt.Errorf("Duplicate func_id %d", entry.FuncId)
		}
		fc.entriesByFuncId[entry.FuncId] = entry
		if fc.entriesByFuncName[entry.FuncName] != nil {
			return nil, fmt.Errorf("Duplicate func_name %d", entry.FuncName)
		}
		if strings.HasPrefix(entry.FuncName, "grpc:") {
			serviceName := strings.TrimPrefix(entry.FuncName, "grpc:")
			log.Printf("[INFO] Load configuration for gRPC service %s", serviceName)
			for _, methodName := range entry.GrpcMethods {
				log.Printf("[INFO] Register method %s for gRPC service %s", methodName, serviceName)
			}
		} else {
			log.Printf("[INFO] Load configuration for function %s[%d]", entry.FuncName, entry.FuncId)
		}
		fc.entriesByFuncName[entry.FuncName] = entry
	}
	return fc, nil
}

func (fc *FuncConfig) findByFuncName(funcName string) *FuncConfigEntry {
	return fc.entriesByFuncName[funcName]
}

func (fc *FuncConfig) findByFuncId(funcId uint16) *FuncConfigEntry {
	return fc.entriesByFuncId[funcId]
}

func (fcEntry *FuncConfigEntry) hasGrpcMethod(method string) bool {
	for _, methodName := range fcEntry.GrpcMethods {
		if methodName == method {
			return true
		}
	}
	return false
}
