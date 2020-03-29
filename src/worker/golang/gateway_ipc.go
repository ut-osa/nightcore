package main

import (
	"fmt"
	"log"
	"net"
)

type GatewayEndpoint struct {
	worker      *Worker
	conn        net.Conn
	funcId      uint16
	clientId    uint16
	messageChan chan Message
}

func newGatewayEndpoint(worker *Worker, addr string) (*GatewayEndpoint, error) {
	c, err := net.Dial("unix", addr)
	if err != nil {
		return nil, err
	}
	return &GatewayEndpoint{
		worker:      worker,
		conn:        c,
		funcId:      worker.funcId,
		clientId:    0,
		messageChan: make(chan Message),
	}, nil
}

func (g *GatewayEndpoint) handshake() error {
	n, err := g.conn.Write(serializeHandshakeMessage(HandshakeMessage{
		role:   Role_FUNC_WORKER,
		funcId: g.funcId,
	}))
	if err != nil {
		return err
	} else if n != HandshakeMessageByteSize {
		return fmt.Errorf("Failed to write handshake message")
	}
	buf := make([]byte, HandshakeResponseByteSize)
	n, err = g.conn.Read(buf)
	if err != nil {
		return err
	} else if n != HandshakeResponseByteSize {
		return fmt.Errorf("Unexpcted size for handshake response")
	}
	response := parseHandshakeResponse(buf)
	g.clientId = response.clientId
	log.Printf("[INFO] Handshake with gateway done: clientId = %d", g.clientId)
	return nil
}

func (g *GatewayEndpoint) readMessageRoutine() {
	defer g.worker.wg.Done()
	buf := make([]byte, MessageByteSize)
	for {
		n, err := g.conn.Read(buf)
		if err != nil {
			log.Fatal("[FATAL] Failed to read gateway message: ", err)
		} else if n != MessageByteSize {
			log.Fatal("[FATAL] Failed to read gateway message")
		}
		g.worker.onGatewayMessage(parseMessage(buf))
	}
}

func (g *GatewayEndpoint) writeMessageRoutine() {
	defer g.worker.wg.Done()
	for {
		message, more := <-g.messageChan
		if more {
			n, err := g.conn.Write(serializeMessage(message))
			if err != nil {
				log.Fatal("[FATAL] Failed to write gateway message: ", err)
			} else if n != MessageByteSize {
				log.Fatal("[FATAL] Failed to write gateway message")
			}
		} else {
			break
		}
	}
}

func (g *GatewayEndpoint) startRoutines() {
	g.worker.wg.Add(2)
	go g.writeMessageRoutine()
	go g.readMessageRoutine()
}

func (g *GatewayEndpoint) writeMessage(message Message) {
	g.messageChan <- message
}

func (g *GatewayEndpoint) close() {
	g.conn.Close()
	close(g.messageChan)
}
