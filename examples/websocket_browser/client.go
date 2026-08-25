//go:build js && wasm

package main

import (
	"fmt"
	"syscall/js"
	"time"

	"github.com/z46-dev/arctic"
)

func setStatus(message string) {
	js.Global().Get("document").Call("getElementById", "status").Set("textContent", message)
}

func main() {
	var (
		client *arctic.Client
		err    error
	)

	setStatus("Connecting to Arctic...")

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: "ws://127.0.0.1:8081/arctic",
		Protocol:      arctic.ProtocolWebSocket,
		BufferSize:    1024,
		Timeout:       5 * time.Second,
		Metadata:      map[string]any{"example": "browser-wasm"},
	}); err != nil {
		setStatus(fmt.Sprintf("Create client: %v", err))
		return
	}

	client.OnMessage(func(message []byte) {
		setStatus("Received: " + string(message))
	})

	client.OnError(func(err error) {
		setStatus(fmt.Sprintf("WebSocket error: %v", err))
	})

	client.OnClose(func() {
		setStatus("Connection closed")
	})

	if err = client.Connect(); err != nil {
		setStatus(fmt.Sprintf("Connect: %v", err))
		return
	}

	setStatus("Connected; waiting for the server...")
	if err = client.Send([]byte("Hello from Go running as WebAssembly")); err != nil {
		setStatus(fmt.Sprintf("Send: %v", err))
		return
	}

	select {}
}
