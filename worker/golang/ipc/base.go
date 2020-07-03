package ipc

import (
	"fmt"
)

var rootPathForIpc string

const fileCreatMode = 0664

func SetRootPathForIpc(path string) {
	rootPathForIpc = path
}

func GetEngineUnixSocketPath() string {
	return fmt.Sprintf("%s/engine.sock", rootPathForIpc)
}

func GetFuncWorkerInputFifoName(clientId uint16) string {
	return fmt.Sprintf("worker_%d_input", clientId)
}

func GetFuncWorkerOutputFifoName(clientId uint16) string {
	return fmt.Sprintf("worker_%d_output", clientId)
}

func GetFuncCallInputShmName(fullCallId uint64) string {
	return fmt.Sprintf("%d.i", fullCallId)
}

func GetFuncCallOutputShmName(fullCallId uint64) string {
	return fmt.Sprintf("%d.o", fullCallId)
}

func GetFuncCallOutputFifoName(fullCallId uint64) string {
	return fmt.Sprintf("%d.o", fullCallId)
}
