package main

import (
	"fmt"
	"log"
	"os"
)

type WatchdogEndpoint struct {
	worker      *Worker
	inputPipe   *os.File
	outputPipe  *os.File
	messageChan chan Message
}

func newWatchdogEndpoint(worker *Worker, inputPipeFd int, outputPipeFd int) (*WatchdogEndpoint, error) {
	inputPipe := os.NewFile(uintptr(inputPipeFd), "input")
	if inputPipe == nil {
		return nil, fmt.Errorf("Invalid input pipe fd %d", inputPipeFd)
	}
	outputPipe := os.NewFile(uintptr(outputPipeFd), "output")
	if outputPipe == nil {
		inputPipe.Close()
		return nil, fmt.Errorf("Invalid output pipe fd %d", outputPipeFd)
	}
	return &WatchdogEndpoint{
		worker:      worker,
		inputPipe:   inputPipe,
		outputPipe:  outputPipe,
		messageChan: make(chan Message),
	}, nil
}

func (w *WatchdogEndpoint) readMessageRoutine() {
	defer w.worker.wg.Done()
	buf := make([]byte, MessageByteSize)
	for {
		n, err := w.inputPipe.Read(buf)
		if err != nil {
			log.Fatal("[FATAL] Failed to read watchdog message: ", err)
		} else if n != MessageByteSize {
			log.Fatal("[FATAL] Failed to read watchdog message")
		}
		w.worker.onWatchdogMessage(parseMessage(buf))
	}
}

func (w *WatchdogEndpoint) writeMessageRoutine() {
	defer w.worker.wg.Done()
	for {
		message, more := <-w.messageChan
		if more {
			n, err := w.outputPipe.Write(serializeMessage(message))
			if err != nil {
				log.Fatal("[FATAL] Failed to write watchdog message: ", err)
			} else if n != MessageByteSize {
				log.Fatal("[FATAL] Failed to write watchdog message")
			}
		} else {
			break
		}
	}
}

func (w *WatchdogEndpoint) startRoutines() {
	w.worker.wg.Add(2)
	go w.writeMessageRoutine()
	go w.readMessageRoutine()
}

func (w *WatchdogEndpoint) writeMessage(message Message) {
	w.messageChan <- message
}

func (w *WatchdogEndpoint) close() {
	w.inputPipe.Close()
	w.outputPipe.Close()
	close(w.messageChan)
}
