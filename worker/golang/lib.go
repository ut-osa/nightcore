package faas

import (
	"encoding/binary"
	"log"
	"os"
	"runtime"
	"strconv"

	config "cs.utexas.edu/zjia/faas/config"
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
	clientId, err := strconv.Atoi(os.Getenv("FAAS_CLIENT_ID"))
	if err != nil {
		log.Fatal("[FATAL] Failed to parse FAAS_CLIENT_ID")
	}
	msgPipeFd, err := strconv.Atoi(os.Getenv("FAAS_MSG_PIPE_FD"))
	if err != nil {
		log.Fatal("[FATAL] Failed to parse FAAS_MSG_PIPE_FD")
	}

	msgPipe := os.NewFile(uintptr(msgPipeFd), "msg_pipe")
	payloadSizeBuf := make([]byte, 4)
	nread, err := msgPipe.Read(payloadSizeBuf)
	if err != nil || nread != len(payloadSizeBuf) {
		log.Fatal("[FATAL] Failed to read payload size")
	}
	payloadSize := binary.LittleEndian.Uint32(payloadSizeBuf)
	payload := make([]byte, payloadSize)
	nread, err = msgPipe.Read(payload)
	if err != nil || nread != len(payload) {
		log.Fatal("[FATAL] Failed to read payload")
	}
	err = config.InitFuncConfig(payload)
	if err != nil {
		log.Fatal("[FATAL] InitFuncConfig failed: %s", err)
	}
	w, err := worker.NewFuncWorker(uint16(funcId), uint16(clientId), factory)
	if err != nil {
		log.Fatal("[FATAL] Failed to create FuncWorker: ", err)
	}
	numWorkers := 1
	go func(w *worker.FuncWorker) {
		w.Run()
	}(w)

	maxProcFactor, err := strconv.Atoi(os.Getenv("FAAS_GO_MAX_PROC_FACTOR"))
	if err != nil {
		maxProcFactor = 8
	}

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
			numWorkers += 1
			planedMaxProcs := (numWorkers - 1) / maxProcFactor + 1
			currentMaxProcs := runtime.GOMAXPROCS(0)
			if planedMaxProcs > currentMaxProcs {
				runtime.GOMAXPROCS(planedMaxProcs)
				log.Printf("[INFO] Current GOMAXPROCS is %d", planedMaxProcs)
			}
			go func(w *worker.FuncWorker) {
				w.Run()
			}(w)
		} else {
			log.Fatal("[FATAL] Unknown message type")
		}
	}
}
