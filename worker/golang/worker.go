package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
)

type WorkerConfig struct {
	funcLibraryPath string
	funcConfigPath  string
	gatewayIpcAddr  string
	funcId          int
	inputPipeFd     int
	outputPipeFd    int
	shmBasePath     string
}

type Worker struct {
	funcLibrary     *FuncLibrary
	funcId          uint16
	funcConfig      *FuncConfig
	shm             *SharedMemory
	gateway         *GatewayEndpoint
	watchdog        *WatchdogEndpoint
	wg              sync.WaitGroup
	nextCallId      uint32
	funcInvocations sync.Map
}

type FuncInvocation struct {
	ctx        context.Context
	resultChan chan bool
}

func newWorker(config WorkerConfig) (*Worker, error) {
	w := new(Worker)
	w.funcId = uint16(config.funcId)
	funcConfig, err := newFuncConfig(config.funcConfigPath)
	if err != nil {
		return nil, err
	}
	w.funcConfig = funcConfig
	funcConfigEntry := w.funcConfig.findByFuncId(w.funcId)
	if funcConfigEntry == nil {
		return nil, fmt.Errorf("Cannot find func_id %d in func_config file", w.funcId)
	}
	funcLibrary, err := newFuncLibrary(config.funcLibraryPath, funcConfigEntry)
	if err != nil {
		return nil, err
	}
	w.funcLibrary = funcLibrary
	shm, err := newSharedMemory(config.shmBasePath)
	if err != nil {
		return nil, err
	}
	w.shm = shm
	gateway, err := newGatewayEndpoint(w, config.gatewayIpcAddr)
	if err != nil {
		return nil, err
	}
	w.gateway = gateway
	watchdog, err := newWatchdogEndpoint(w, config.inputPipeFd, config.outputPipeFd)
	if err != nil {
		gateway.close()
		return nil, err
	}
	w.watchdog = watchdog
	return w, nil
}

func (w *Worker) handshakeWithGateway() error {
	return w.gateway.handshake()
}

// Implement types.Environment
func (w *Worker) InvokeFunc(ctx context.Context, funcName string, input []byte) ([]byte, error) {
	if strings.HasPrefix(funcName, "grpc:") {
		return nil, fmt.Errorf("Function handler should use GrpcCall to invoke gRPC methods")
	}
	return w.invokeFunc(ctx, funcName, input)
}

func (w *Worker) GrpcCall(ctx context.Context, service string, method string, request []byte) ([]byte, error) {
	return w.grpcCall(ctx, service, method, request)
}

func (w *Worker) serve() {
	w.funcLibrary.init(w)
	w.gateway.startRoutines()
	w.watchdog.startRoutines()
}

func (w *Worker) waitForFinish() {
	w.wg.Wait()
}

func (w *Worker) runFuncHandler(funcCall FuncCall) bool {
	inputShm, err := w.shm.openReadOnly(fmt.Sprintf("%d.i", fullFuncCallId(funcCall)))
	if err != nil {
		log.Print("[ERROR] Failed to open shared memory: ", err)
		return false
	}
	input := make([]byte, len(inputShm))
	if copy(input, inputShm) != len(inputShm) {
		log.Fatal("[FATAL] Failed to copy input from shared memory")
	}
	w.shm.close(inputShm)
	output, err := w.funcLibrary.funcCall(context.Background(), input)
	if err != nil {
		return false
	}
	outputShm, err := w.shm.create(fmt.Sprintf("%d.o", fullFuncCallId(funcCall)), len(output))
	if err != nil {
		log.Print("[ERROR] Failed to create shared memory: ", err)
		return false
	}
	if copy(outputShm, output) != len(output) {
		log.Fatal("[FATAL] Failed to copy output to shared memory")
	}
	w.shm.close(outputShm)
	return true
}

func (w *Worker) invokeFunc(ctx context.Context, funcName string, input []byte) ([]byte, error) {
	funcConfigEntry := w.funcConfig.findByFuncName(funcName)
	if funcConfigEntry == nil {
		return nil, fmt.Errorf("Cannot find function with name %s", funcName)
	}
	if len(input) == 0 {
		return nil, fmt.Errorf("Empty function input")
	}
	funcCall := FuncCall{
		funcId:   funcConfigEntry.FuncId,
		clientId: w.gateway.clientId,
		callId:   atomic.AddUint32(&w.nextCallId, 1),
	}
	inputShm, err := w.shm.create(fmt.Sprintf("%d.i", fullFuncCallId(funcCall)), len(input))
	if err != nil {
		log.Print("[ERROR] Failed to open shared memory: ", err)
		return nil, fmt.Errorf("Internal error")
	}
	if copy(inputShm, input) != len(input) {
		log.Fatal("[FATAL] Failed to copy input to shared memory")
	}
	w.shm.close(inputShm)
	fi := FuncInvocation{
		ctx:        ctx,
		resultChan: make(chan bool),
	}
	defer close(fi.resultChan)
	w.funcInvocations.Store(fullFuncCallId(funcCall), &fi)
	w.gateway.writeMessage(Message{
		messageType: MessageType_INVOKE_FUNC,
		funcCall:    funcCall,
	})
	var success bool
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case success = <-fi.resultChan:
		break
	}
	w.funcInvocations.Delete(fullFuncCallId(funcCall))
	w.shm.remove(fmt.Sprintf("%d.i", fullFuncCallId(funcCall)))
	if !success {
		return nil, fmt.Errorf("Function call failed")
	}
	outputShm, err := w.shm.openReadOnly(fmt.Sprintf("%d.o", fullFuncCallId(funcCall)))
	if err != nil {
		log.Print("[ERROR] Failed to open shared memory: ", err)
		return nil, fmt.Errorf("Internal error")
	}
	output := make([]byte, len(outputShm))
	if copy(output, outputShm) != len(outputShm) {
		log.Fatal("[FATAL] Failed to copy output from shared memory")
	}
	w.shm.close(outputShm)
	w.shm.remove(fmt.Sprintf("%d.o", fullFuncCallId(funcCall)))
	return output, nil
}

func (w *Worker) grpcCall(ctx context.Context, service string, method string, request []byte) ([]byte, error) {
	funcName := "grpc:" + service
	funcConfigEntry := w.funcConfig.findByFuncName(funcName)
	if funcConfigEntry == nil {
		return nil, fmt.Errorf("Cannot find gRPC service %s", service)
	}
	if !funcConfigEntry.hasGrpcMethod(method) {
		return nil, fmt.Errorf("Cannot find method %s for gRPC service %s", method, service)
	}
	var input bytes.Buffer
	input.WriteString(method)
	input.WriteByte('\x00')
	input.Write(request)
	return w.invokeFunc(ctx, funcName, input.Bytes())
}

func (w *Worker) onWatchdogMessage(message Message) {
	if message.messageType == MessageType_INVOKE_FUNC {
		if message.funcCall.funcId != w.funcId {
			log.Fatalf("[FATAL] Cannot invoke function of func_id %d", message.funcCall.funcId)
		}
		go func() {
			success := w.runFuncHandler(message.funcCall)
			var response Message
			response.funcCall = message.funcCall
			if success {
				response.messageType = MessageType_FUNC_CALL_COMPLETE
			} else {
				response.messageType = MessageType_FUNC_CALL_FAILED
			}
			if success {
				w.gateway.writeMessage(response)
			}
			w.watchdog.writeMessage(response)
		}()
	} else {
		log.Print("[ERROR] Unknown message type")
	}
}

func (w *Worker) onGatewayMessage(message Message) {
	if message.messageType == MessageType_FUNC_CALL_COMPLETE || message.messageType == MessageType_FUNC_CALL_FAILED {
		value, exist := w.funcInvocations.Load(fullFuncCallId(message.funcCall))
		if !exist {
			log.Printf("[ERROR] Cannot find InvokeContext for call_id %d", fullFuncCallId(message.funcCall))
			return
		}
		fi := value.(*FuncInvocation)
		if message.messageType == MessageType_FUNC_CALL_COMPLETE {
			fi.resultChan <- true
		} else {
			fi.resultChan <- false
		}
	} else {
		log.Print("[ERROR] Unknown message type")
	}
}

func (w *Worker) close() {
	w.gateway.close()
	w.watchdog.close()
}
