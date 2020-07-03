package faas

import (
	"log"
	"os"
	"runtime"
	"strconv"

	ipc "cs.utexas.edu/zjia/faas/ipc"
	protocol "cs.utexas.edu/zjia/faas/protocol"
	types "cs.utexas.edu/zjia/faas/types"
	worker "cs.utexas.edu/zjia/faas/worker"
)

func Serve(factory types.FuncHandlerFactory) {
	runtime.GOMAXPROCS(1)
	ipc.SetRootPathForIpc(os.Getenv("FAAS_ROOT_PATH_FOR_IPC"))
	funcId, err := strconv.Atoi(os.Getenv("FAAS_FUNC_ID"))
	if err != nil {
		log.Fatal("[FATAL] Failed to parse FAAS_FUNC_ID")
	}
	msgPipeFd, err := strconv.Atoi(os.Getenv("FAAS_MSG_PIPE_FD"))
	if err != nil {
		log.Fatal("[FATAL] Failed to parse FAAS_MSG_PIPE_FD")
	}

	msgPipe := os.NewFile(uintptr(msgPipeFd), "msg_pipe")
	for {
		message := protocol.NewEmptyMessage()
		nread, err := msgPipe.Read(message)
		if err != nil || nread != protocol.MessageFullByteSize {
			log.Fatal("[FATAL] Failed to read launcher message")
		}
		if protocol.IsCreateFuncWorkerMessage(message) {
			clientId := protocol.GetClientIdFromMessage(message)
			w, err := worker.NewFuncWorker(uint16(funcId), clientId, factory)
			if err != nil {
				log.Fatal("[FATAL] Failed to create FuncWorker: ", err)
			}
			maxProcs := runtime.GOMAXPROCS(0)
			runtime.GOMAXPROCS(maxProcs + 1)
			log.Printf("[INFO] Current GOMAXPROCS is %d", maxProcs + 1)
			go func(w *worker.FuncWorker) {
				w.Run()
			}(w)
		} else {
			log.Fatal("[FATAL] Unknown message type")
		}
	}
}
