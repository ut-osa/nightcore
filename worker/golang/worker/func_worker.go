package worker

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	common "cs.utexas.edu/zjia/faas/common"
	config "cs.utexas.edu/zjia/faas/config"
	ipc "cs.utexas.edu/zjia/faas/ipc"
	protocol "cs.utexas.edu/zjia/faas/protocol"
	types "cs.utexas.edu/zjia/faas/types"
)

const PIPE_BUF = 4096

type FuncWorker struct {
	funcId               uint16
	clientId             uint16
	factory              types.FuncHandlerFactory
	configEntry          *config.FuncConfigEntry
	isGrpcSrv            bool
	useFifoForNestedCall bool
	engineConn           net.Conn
	newFuncCallChan      chan []byte
	inputPipe            *os.File
	outputPipe           *os.File                 // protected by mux
	outgoingFuncCalls    map[uint64](chan []byte) // protected by mux
	handler              types.FuncHandler
	grpcHandler          types.GrpcFuncHandler
	nextCallId           uint32
	currentCall          uint64
	mux                  sync.Mutex
}

func NewFuncWorker(funcId uint16, clientId uint16, factory types.FuncHandlerFactory) (*FuncWorker, error) {
	w := &FuncWorker{
		funcId:               funcId,
		clientId:             clientId,
		factory:              factory,
		isGrpcSrv:            false,
		useFifoForNestedCall: false,
		newFuncCallChan:      make(chan []byte),
		outgoingFuncCalls:    make(map[uint64](chan []byte)),
		nextCallId:           0,
		currentCall:          0,
	}
	return w, nil
}

func (w *FuncWorker) Run() {
	log.Printf("[INFO] Start new FuncWorker with client id %d", w.clientId)
	err := w.doHandshake()
	if err != nil {
		log.Fatalf("[FATAL] Handshake failed: %v", err)
	}
	log.Printf("[INFO] Handshake with engine done")

	go w.servingLoop()
	for {
		message := protocol.NewEmptyMessage()
		n, err := w.inputPipe.Read(message)
		if err != nil || n != protocol.MessageFullByteSize {
			log.Fatal("[FATAL] Failed to read engine message")
		}
		if protocol.IsDispatchFuncCallMessage(message) {
			w.newFuncCallChan <- message
		} else if protocol.IsFuncCallCompleteMessage(message) || protocol.IsFuncCallFailedMessage(message) {
			funcCall := protocol.GetFuncCallFromMessage(message)
			w.mux.Lock()
			ch, exists := w.outgoingFuncCalls[funcCall.FullCallId()]
			if exists {
				ch <- message
				delete(w.outgoingFuncCalls, funcCall.FullCallId())
			}
			w.mux.Unlock()
		} else {
			log.Fatal("[FATAL] Unknown message type")
		}
	}
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

	flags := protocol.GetFlagsFromMessage(response)
	if (flags & protocol.FLAG_UseFifoForNestedCall) != 0 {
		log.Printf("[INFO] Use FIFO for nested calls")
		w.useFifoForNestedCall = true
	}

	w.configEntry = config.FindByFuncId(w.funcId)
	if w.configEntry == nil {
		return fmt.Errorf("Invalid funcId: %d", w.funcId)
	}
	w.isGrpcSrv = strings.HasPrefix(w.configEntry.FuncName, "grpc:")

	if w.isGrpcSrv {
		handler, err := w.factory.GrpcNew(w, strings.TrimPrefix(w.configEntry.FuncName, "grpc:"))
		if err != nil {
			return err
		}
		w.grpcHandler = handler
	} else {
		handler, err := w.factory.New(w, w.configEntry.FuncName)
		if err != nil {
			return err
		}
		w.handler = handler
	}

	op, err := ipc.FifoOpenForWrite(ipc.GetFuncWorkerOutputFifoName(w.clientId), false)
	if err != nil {
		return err
	}
	w.outputPipe = op

	return nil
}

func (w *FuncWorker) servingLoop() {
	for {
		message := <-w.newFuncCallChan
		w.executeFunc(message)
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
			w.mux.Lock()
			_, err = w.outputPipe.Write(response)
			w.mux.Unlock()
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
	atomic.StoreUint64(&w.currentCall, funcCall.FullCallId())
	startTimestamp := common.GetMonotonicMicroTimestamp()
	if w.isGrpcSrv {
		output, err = w.grpcHandler.Call(context.Background(), methodName, input)
	} else {
		output, err = w.handler.Call(context.Background(), input)
	}
	processingTime := common.GetMonotonicMicroTimestamp() - startTimestamp
	atomic.StoreUint64(&w.currentCall, 0)
	if err != nil {
		log.Printf("[ERROR] FuncCall failed with error: %v", err)
	}

	var response []byte
	if w.useFifoForNestedCall {
		response = w.fifoFuncCallFinished(funcCall, err == nil, output, int32(processingTime))
	} else {
		response = w.funcCallFinished(funcCall, err == nil, output, int32(processingTime))
	}
	protocol.SetDispatchDelayInMessage(response, int32(dispatchDelay))
	protocol.SetSendTimestampInMessage(response, common.GetMonotonicMicroTimestamp())
	w.mux.Lock()
	_, err = w.outputPipe.Write(response)
	w.mux.Unlock()
	if err != nil {
		log.Fatal("[FATAL] Failed to write engine message!")
	}
}

func (w *FuncWorker) funcCallFinished(funcCall protocol.FuncCall, success bool, output []byte, processingTime int32) []byte {
	var response []byte
	if success {
		response = protocol.NewFuncCallCompleteMessage(funcCall, processingTime)
		if len(output) > protocol.MessageInlineDataSize {
			err := w.writeOutputToShm(funcCall, output)
			if err != nil {
				log.Printf("[ERROR] writeOutputToShm failed: %v", err)
				response = protocol.NewFuncCallFailedMessage(funcCall)
			} else {
				protocol.SetPayloadSizeInMessage(response, int32(-len(output)))
			}
		} else if len(output) > 0 {
			protocol.FillInlineDataInMessage(response, output)
		}
	} else {
		response = protocol.NewFuncCallFailedMessage(funcCall)
	}
	return response
}

func (w *FuncWorker) fifoFuncCallFinished(funcCall protocol.FuncCall, success bool, output []byte, processingTime int32) []byte {
	var response []byte
	if success {
		response = protocol.NewFuncCallCompleteMessage(funcCall, processingTime)
	} else {
		response = protocol.NewFuncCallFailedMessage(funcCall)
	}

	if funcCall.ClientId == 0 {
		// FuncCall from engine directly
		if success {
			if len(output) > protocol.MessageInlineDataSize {
				err := w.writeOutputToShm(funcCall, output)
				if err != nil {
					log.Printf("[ERROR] writeOutputToShm failed: %v", err)
					response = protocol.NewFuncCallFailedMessage(funcCall)
				} else {
					protocol.SetPayloadSizeInMessage(response, int32(-len(output)))
				}
			} else if len(output) > 0 {
				protocol.FillInlineDataInMessage(response, output)
			}
		}
	} else {
		// FuncCall from another FuncWorker
		err := w.writeOutputToFifo(funcCall, success, output)
		if err != nil {
			log.Printf("[ERROR] writeOutputToFifo failed: %v", err)
			response = protocol.NewFuncCallFailedMessage(funcCall)
		} else if success {
			protocol.SetPayloadSizeInMessage(response, int32(len(output)))
		}
	}

	return response
}

func (w *FuncWorker) writeOutputToShm(funcCall protocol.FuncCall, output []byte) error {
	shmName := ipc.GetFuncCallOutputShmName(funcCall.FullCallId())
	outputRegion, err := ipc.ShmCreate(shmName, len(output))
	if err != nil {
		return err
	}
	defer outputRegion.Close()
	copy(outputRegion.Data, output)
	return nil
}

func (w *FuncWorker) writeOutputToFifo(funcCall protocol.FuncCall, success bool, output []byte) error {
	fifo, err := ipc.FifoOpenForWrite(ipc.GetFuncCallOutputFifoName(funcCall.FullCallId()), true)
	if err != nil {
		return err
	}
	defer fifo.Close()
	var buffer []byte
	if success {
		if len(output)+4 > PIPE_BUF {
			err := w.writeOutputToShm(funcCall, output)
			if err != nil {
				return err
			}
			buffer = make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(len(output)))
		} else {
			buffer = make([]byte, len(output)+4)
			binary.LittleEndian.PutUint32(buffer[0:4], uint32(len(output)))
			copy(buffer[4:], output)
		}
	} else {
		buffer = make([]byte, 4)
		header := int32(-1)
		binary.LittleEndian.PutUint32(buffer, uint32(header))
	}
	_, err = fifo.Write(buffer)
	return err
}

func (w *FuncWorker) newFuncCallCommon(funcCall protocol.FuncCall, input []byte) ([]byte, error) {
	message := protocol.NewInvokeFuncCallMessage(funcCall, atomic.LoadUint64(&w.currentCall))

	var inputRegion *ipc.ShmRegion
	var outputFifo *os.File
	var outputChan chan []byte
	var output []byte
	var err error

	if len(input) > protocol.MessageInlineDataSize {
		inputRegion, err = ipc.ShmCreate(ipc.GetFuncCallInputShmName(funcCall.FullCallId()), len(input))
		if err != nil {
			return nil, fmt.Errorf("ShmCreate failed: %v", err)
		}
		defer func() {
			inputRegion.Close()
			inputRegion.Remove()
		}()
		copy(inputRegion.Data, input)
		protocol.SetPayloadSizeInMessage(message, int32(-len(input)))
	} else {
		protocol.FillInlineDataInMessage(message, input)
	}

	if w.useFifoForNestedCall {
		outputFifoName := ipc.GetFuncCallOutputFifoName(funcCall.FullCallId())
		err = ipc.FifoCreate(outputFifoName)
		if err != nil {
			return nil, fmt.Errorf("FifoCreate failed: %v", err)
		}
		defer ipc.FifoRemove(outputFifoName)
		outputFifo, err = ipc.FifoOpenForReadWrite(outputFifoName, true)
		if err != nil {
			return nil, fmt.Errorf("FifoOpenForReadWrite failed: %v", err)
		}
		defer outputFifo.Close()
	}

	w.mux.Lock()
	if !w.useFifoForNestedCall {
		outputChan = make(chan []byte)
		w.outgoingFuncCalls[funcCall.FullCallId()] = outputChan
	}
	_, err = w.outputPipe.Write(message)
	w.mux.Unlock()

	if w.useFifoForNestedCall {
		headerBuf := make([]byte, 4)
		nread, err := outputFifo.Read(headerBuf)
		if err != nil {
			return nil, fmt.Errorf("Failed to read from fifo: %v", err)
		} else if nread < len(headerBuf) {
			return nil, fmt.Errorf("Failed to read header from output fifo")
		}

		header := int32(binary.LittleEndian.Uint32(headerBuf))
		if header < 0 {
			return nil, fmt.Errorf("FuncCall failed")
		}

		outputSize := int(header)
		output = make([]byte, outputSize)
		if outputSize+4 > PIPE_BUF {
			outputRegion, err := ipc.ShmOpen(ipc.GetFuncCallOutputShmName(funcCall.FullCallId()), true)
			if err != nil {
				return nil, fmt.Errorf("ShmOpen failed: %v", err)
			}
			defer func() {
				outputRegion.Close()
				outputRegion.Remove()
			}()
			if outputRegion.Size != outputSize {
				return nil, fmt.Errorf("Shm size mismatch with header read from output fifo")
			}
			copy(output, outputRegion.Data)
		} else {
			nread, err = outputFifo.Read(output)
			if err != nil {
				return nil, fmt.Errorf("Failed to read from fifo: %v", err)
			} else if nread < outputSize {
				return nil, fmt.Errorf("Failed to read output from fifo")
			}
		}
	} else {
		message := <-outputChan
		if protocol.IsFuncCallFailedMessage(message) {
			return nil, fmt.Errorf("FuncCall failed")
		}
		payloadSize := protocol.GetPayloadSizeFromMessage(message)
		if payloadSize < 0 {
			outputSize := int(-payloadSize)
			output = make([]byte, outputSize)
			outputRegion, err := ipc.ShmOpen(ipc.GetFuncCallOutputShmName(funcCall.FullCallId()), true)
			if err != nil {
				return nil, fmt.Errorf("ShmOpen failed: %v", err)
			}
			defer func() {
				outputRegion.Close()
				outputRegion.Remove()
			}()
			if outputRegion.Size != outputSize {
				return nil, fmt.Errorf("Shm size mismatch with header read from output fifo")
			}
			copy(output, outputRegion.Data)
		} else {
			output = protocol.GetInlineDataFromMessage(message)
		}
	}

	return output, nil
}

// Implement types.Environment
func (w *FuncWorker) InvokeFunc(ctx context.Context, funcName string, input []byte) ([]byte, error) {
	entry := config.FindByFuncName(funcName)
	if entry == nil {
		return nil, fmt.Errorf("Invalid function name: %s", funcName)
	}
	funcCall := protocol.FuncCall{
		FuncId:   entry.FuncId,
		MethodId: 0,
		ClientId: w.clientId,
		CallId:   atomic.AddUint32(&w.nextCallId, 1) - 1,
	}
	return w.newFuncCallCommon(funcCall, input)
}

// Implement types.Environment
func (w *FuncWorker) GrpcCall(ctx context.Context, service string, method string, request []byte) ([]byte, error) {
	entry := config.FindByFuncName("grpc:" + service)
	if entry == nil {
		return nil, fmt.Errorf("Invalid gRPC service: %s", service)
	}
	methodId := entry.FindGrpcMethod(method)
	if methodId < 0 {
		return nil, fmt.Errorf("Invalid gRPC method: %s", method)
	}
	funcCall := protocol.FuncCall{
		FuncId:   entry.FuncId,
		MethodId: uint16(methodId),
		ClientId: w.clientId,
		CallId:   atomic.AddUint32(&w.nextCallId, 1) - 1,
	}
	return w.newFuncCallCommon(funcCall, request)
}
