package worker

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	common "cs.utexas.edu/zjia/faas/common"
	config "cs.utexas.edu/zjia/faas/config"
	ipc "cs.utexas.edu/zjia/faas/ipc"
	protocol "cs.utexas.edu/zjia/faas/protocol"
	types "cs.utexas.edu/zjia/faas/types"
)

type FuncWorker struct {
	funcId      uint16
	clientId    uint16
	handler     types.FuncHandler
	funcConfig  *config.FuncConfig
	configEntry *config.FuncConfigEntry
	isGrpcSrv   bool
	engineConn  net.Conn
	inputPipe   *os.File
	outputPipe  *os.File
	pipeBuf     []byte
}

func NewFuncWorker(funcId uint16, clientId uint16, handler types.FuncHandler) (*FuncWorker, error) {
	w := &FuncWorker{
		funcId:   funcId,
		clientId: clientId,
		handler:  handler,
		pipeBuf:  make([]byte, protocol.PIPE_BUF),
	}
	err := w.handler.Init(w)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (w *FuncWorker) Run() {
	err := w.doHandshake()
	if err != nil {
		log.Fatalf("[FATAL] Handshake failed: %v", err)
	}
	log.Printf("[INFO] Handshake with engine done")
	w.servingLoop()
}

func (w *FuncWorker) doHandshake() error {
	c, err := net.Dial("unix", ipc.GetEngineUnixSocketPath())
	if err != nil {
		return err
	}
	w.engineConn = c

	ip, err := ipc.FifoOpenForRead(ipc.GetFuncWorkerInputFifoName(w.clientId), true)
	if err != nil {
		return err
	}
	w.inputPipe = ip

	message := protocol.NewFuncWorkerHandshakeMessage(w.funcId, w.clientId)
	_, err = w.engineConn.Write(message)
	if err != nil {
		return err
	}
	response := protocol.NewEmptyMessage()
	n, err := w.engineConn.Read(response)
	if err != nil {
		return err
	} else if n != protocol.MessageFullByteSize {
		return fmt.Errorf("Unexpcted size for handshake response")
	} else if !protocol.IsHandshakeResponseMessage(response) {
		return fmt.Errorf("Unexpcted type of response")
	}
	payloadSize := protocol.GetPayloadSizeFromMessage(response)
	if payloadSize <= 0 {
		return fmt.Errorf("Unexpcted payload size of response")
	}
	payload := make([]byte, payloadSize)
	n, err = w.engineConn.Read(payload)
	if err != nil {
		return err
	} else if n != int(payloadSize) {
		return fmt.Errorf("Failed to read payload of handshake")
	}
	fc, err := config.NewFuncConfig(payload)
	if err != nil {
		return err
	}
	w.funcConfig = fc

	w.configEntry = fc.FindByFuncId(w.funcId)
	if w.configEntry == nil {
		return fmt.Errorf("Invalid funcId: %d", w.funcId)
	}
	w.isGrpcSrv = strings.HasPrefix(w.configEntry.FuncName, "grpc:")

	op, err := ipc.FifoOpenForWrite(ipc.GetFuncWorkerOutputFifoName(w.clientId), false)
	if err != nil {
		return err
	}
	w.outputPipe = op

	return nil
}

func (w *FuncWorker) servingLoop() {
	for {
		message := protocol.NewEmptyMessage()
		n, err := w.inputPipe.Read(message)
		if err != nil || n != protocol.MessageFullByteSize {
			log.Fatal("[FATAL] Failed to read engine message")
		}
		if protocol.IsDispatchFuncCallMessage(message) {
			w.executeFunc(message)
		} else {
			log.Fatal("[FATAL] Unknown message type")
		}
	}
}

func (w *FuncWorker) executeFunc(dispatchFuncMessage []byte) {
	dispatchDelay := common.GetMonotonicMicroTimestamp() - protocol.GetSendTimestampFromMessage(dispatchFuncMessage)
	funcCall := protocol.GetFuncCallFromMessage(dispatchFuncMessage)

	var input []byte
	var inputRegion *ipc.ShmRegion
	var err error

	if protocol.GetPayloadSizeFromMessage(dispatchFuncMessage) < 0 {
		shmName := ipc.GetFuncCallInputShmName(funcCall.FullCallId())
		inputRegion, err = ipc.ShmOpen(shmName, true)
		if err != nil {
			log.Printf("[ERROR] ShmOpen %s failed: %v", shmName, err)
			response := protocol.NewFuncCallFailedMessage(funcCall)
			protocol.SetSendTimestampInMessage(response, common.GetMonotonicMicroTimestamp())
			_, err = w.outputPipe.Write(response)
			if err != nil {
				log.Fatal("[FATAL] Failed to write engine message!")
			}
			return
		}
		defer inputRegion.Close()
		input = inputRegion.Data
	} else {
		input = protocol.GetInlineDataFromMessage(dispatchFuncMessage)
	}

	methodName := ""
	if w.isGrpcSrv {
		methodId := int(funcCall.MethodId)
		if methodId < len(w.configEntry.GrpcMethods) {
			methodName = w.configEntry.GrpcMethods[methodId]
		} else {
			log.Fatalf("[FATAL] Invalid methodId: %s", funcCall.MethodId)
		}
	}

	var output []byte

	startTimestamp := common.GetMonotonicMicroTimestamp()
	if w.isGrpcSrv {
		output, err = w.handler.GrpcCall(context.Background(), methodName, input)
	} else {
		output, err = w.handler.Call(context.Background(), input)
	}
	processingTime := common.GetMonotonicMicroTimestamp() - startTimestamp

	success := true
	if err != nil {
		success = false
		log.Printf("[ERROR] FuncCall failed with error: %v", err)
	}

	if funcCall.ClientId > 0 {
		log.Fatal("[FATAL] Not implemented!")
	}

	var response []byte
	if success {
		response = protocol.NewFuncCallCompleteMessage(funcCall, int32(processingTime))
		if len(output) > protocol.MessageInlineDataSize {
			shmName := ipc.GetFuncCallOutputShmName(funcCall.FullCallId())
			outputRegion, err := ipc.ShmCreate(shmName, len(output))
			if err != nil {
				log.Printf("[ERROR] ShmCreate %s failed: %v", shmName, err)
				response = protocol.NewFuncCallFailedMessage(funcCall)
			} else {
				copy(outputRegion.Data, output)
				outputRegion.Close()
				protocol.SetPayloadSizeInMessage(response, int32(-len(output)))
			}
		} else if len(output) > 0 {
			protocol.FillInlineDataInMessage(response, output)
		}
	} else {
		response = protocol.NewFuncCallFailedMessage(funcCall)
	}

	protocol.SetDispatchDelayInMessage(response, int32(dispatchDelay))
	protocol.SetSendTimestampInMessage(response, common.GetMonotonicMicroTimestamp())

	_, err = w.outputPipe.Write(response)
	if err != nil {
		log.Fatal("[FATAL] Failed to write engine message!")
	}
}

// Implement types.Environment
func (w *FuncWorker) InvokeFunc(ctx context.Context, funcName string, input []byte) ([]byte, error) {
	return nil, nil
}

// Implement types.Environment
func (w *FuncWorker) GrpcCall(ctx context.Context, service string, method string, request []byte) ([]byte, error) {
	return nil, nil
}
