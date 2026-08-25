package main

import (
	"log"
	"time"

	"github.com/z46-dev/arctic"
)

func main() {
	var (
		server     *arctic.Server
		sidecar    *arctic.WebSocketServer
		client     *arctic.Client
		listenDone chan error    = make(chan error, 1)
		received   chan struct{} = make(chan struct{})
		err        error
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:8080",
		Protocol:    arctic.ProtocolTCP,
		BufferSize:  1024,
		Timeout:     5 * time.Second,
	}); err != nil {
		log.Fatal(err)
	}

	if sidecar, err = arctic.NewWebSocketServer(arctic.WebSocketServerConfig{
		BindAddress: "127.0.0.1:8081",
		Path:        "/arctic",
		Timeout:     5 * time.Second,
	}); err != nil {
		log.Fatal(err)
	}

	if err = server.AddWebSocketSidecar(sidecar); err != nil {
		log.Fatal(err)
	}

	server.OnClient(func(client *arctic.ServerClient) {
		var sendErr error

		log.Printf("client %d connected over %s", client.ID(), client.RemoteAddr())
		client.OnMessage(func(message []byte) {
			log.Printf("server received: %s", message)
			if sendErr = client.Send([]byte("Hello from the Arctic WebSocket sidecar")); sendErr != nil {
				log.Printf("server send failed: %v", sendErr)
			}
		})
	})

	go func() { listenDone <- server.Listen() }()
	defer func() {
		var closeErr error

		if closeErr = server.Close(); closeErr != nil {
			log.Printf("server close failed: %v", closeErr)
		}
	}()

	for sidecar.Addr() == nil {
		time.Sleep(time.Millisecond)
	}

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: "ws://127.0.0.1:8081/arctic",
		Protocol:      arctic.ProtocolWebSocket,
		BufferSize:    1024,
		Timeout:       5 * time.Second,
		Metadata:      map[string]any{"example": "native-websocket"},
	}); err != nil {
		log.Fatal(err)
	}
	
	defer client.Close()

	client.OnMessage(func(message []byte) {
		log.Printf("native client received: %s", message)
		close(received)
	})
	
	if err = client.Connect(); err != nil {
		log.Fatal(err)
	}
	
	if err = client.Send([]byte("Hello from the native WebSocket client")); err != nil {
		log.Fatal(err)
	}

	select {
	case <-received:
	case err = <-listenDone:
		if err != nil {
			log.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		log.Fatal("timed out waiting for the WebSocket response")
	}
}
