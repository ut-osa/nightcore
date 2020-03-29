package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s [func_library_path]\n", os.Args[0])
		os.Exit(1)
	}
	funcId, err := strconv.Atoi(os.Getenv("FUNC_ID"))
	if err != nil {
		log.Fatal("[FATAL] Failed to parse FUNC_ID")
	}
	inputPipeFd, err := strconv.Atoi(os.Getenv("INPUT_PIPE_FD"))
	if err != nil {
		log.Fatal("[FATAL] Failed to parse INPUT_PIPE_FD")
	}
	outputPipeFd, err := strconv.Atoi(os.Getenv("OUTPUT_PIPE_FD"))
	if err != nil {
		log.Fatal("[FATAL] Failed to parse OUTPUT_PIPE_FD")
	}
	w, err := newWorker(WorkerConfig{
		funcLibraryPath: os.Args[1],
		funcConfigPath:  os.Getenv("FUNC_CONFIG_FILE"),
		gatewayIpcAddr:  os.Getenv("GATEWAY_IPC_PATH"),
		funcId:          funcId,
		inputPipeFd:     inputPipeFd,
		outputPipeFd:    outputPipeFd,
		shmBasePath:     os.Getenv("SHARED_MEMORY_PATH"),
	})
	if err != nil {
		log.Fatal("[FATAL] Failed to create worker: ", err)
	}
	defer w.close()
	err = w.handshakeWithGateway()
	if err != nil {
		log.Fatal("[FATAL] Failed to handshake with gateway: ", err)
	}
	w.serve()
	w.waitForFinish()
}
