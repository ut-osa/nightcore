package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
)

type FuncConfigEntry struct {
	FuncName string `json:"funcName"`
	FuncId   uint16 `json:"funcId"`
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
		log.Printf("[INFO] Load configuration for function %s[%d]", entry.FuncName, entry.FuncId)
		if fc.entriesByFuncId[entry.FuncId] != nil {
			return nil, fmt.Errorf("Duplicate func_id %d", entry.FuncId)
		}
		fc.entriesByFuncId[entry.FuncId] = entry
		if fc.entriesByFuncName[entry.FuncName] != nil {
			return nil, fmt.Errorf("Duplicate func_name %d", entry.FuncName)
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
