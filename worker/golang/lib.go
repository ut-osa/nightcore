package faas

import (
	"log"
	"os"
	"runtime"
	"strconv"

	ipc "cs.utexas.edu/zjia/faas/ipc"
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
	w, err := worker.NewFuncWorker(uint16(funcId), uint16(clientId), factory)
	if err != nil {
		log.Fatal("[FATAL] Failed to create FuncWorker: ", err)
	}
	w.Run()
}
