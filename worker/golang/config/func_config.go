package config

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

type FuncConfigEntry struct {
	FuncName    string   `json:"funcName"`
	FuncId      uint16   `json:"funcId"`
	GrpcMethods []string `json:"grpcMethods"`
}

var entries []*FuncConfigEntry
var entriesByFuncId map[uint16]*FuncConfigEntry
var entriesByFuncName map[string]*FuncConfigEntry

func InitFuncConfig(jsonContents []byte) error {
	entries = make([]*FuncConfigEntry, 0)
	err := json.Unmarshal(jsonContents, &entries)
	if err != nil {
		return fmt.Errorf("Failed to unmarshal json: %v", err)
	}
	entriesByFuncId = make(map[uint16]*FuncConfigEntry)
	entriesByFuncName = make(map[string]*FuncConfigEntry)
	for _, entry := range entries {
		if entriesByFuncId[entry.FuncId] != nil {
			return fmt.Errorf("Duplicate func_id %d", entry.FuncId)
		}
		entriesByFuncId[entry.FuncId] = entry
		if entriesByFuncName[entry.FuncName] != nil {
			return fmt.Errorf("Duplicate func_name %d", entry.FuncName)
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
		entriesByFuncName[entry.FuncName] = entry
	}
	return nil
}

func FindByFuncName(funcName string) *FuncConfigEntry {
	return entriesByFuncName[funcName]
}

func FindByFuncId(funcId uint16) *FuncConfigEntry {
	return entriesByFuncId[funcId]
}

func (fcEntry *FuncConfigEntry) FindGrpcMethod(method string) int {
	for idx, methodName := range fcEntry.GrpcMethods {
		if methodName == method {
			return idx
		}
	}
	return -1
}
