package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/z46-dev/arctic"
)

var flagAddr *string = flag.String("addr", "127.0.0.1:8082", "Address for the server to bind to")

func server() {
	var (
		server *arctic.Server
		err    error
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: *flagAddr,
		BufferSize:  1024,
		Timeout:     5 * time.Second,
	}); err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	server.OnClient(func(client *arctic.ServerClient) {
		var (
			logPrefix string         = fmt.Sprintf("[%d | %s]", client.ID(), client.RemoteAddr())
			metadata  map[string]any = client.Metadata()
		)

		log.Printf("%s Client connected with metadata: %#v", logPrefix, metadata)

		client.OnMessage(func(message []byte) {
			var err error

			log.Printf("%s Received message: %s", logPrefix, string(message))

			if err = client.Send([]byte("Metadata received")); err != nil {
				log.Printf("%s Failed to send response: %v", logPrefix, err)
			}
		})

		client.OnError(func(err error) {
			log.Printf("%s Client error: %v", logPrefix, err)
		})
	})

	if err = server.Listen(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func client() {
	var (
		client *arctic.Client
		err    error
	)

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: *flagAddr,
		BufferSize:    1024,
		Timeout:       5 * time.Second,
		Metadata: map[string]any{
			"tenant":   "acme",
			"trace_id": "demo-123",
			"attempt":  1,
		},
	}); err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	client.OnMessage(func(message []byte) {
		log.Printf("Received response: %s", string(message))
	})

	client.OnError(func(err error) {
		log.Printf("Client error: %v", err)
	})

	if err = client.Connect(); err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}

	if err = client.Send([]byte("Hello with metadata")); err != nil {
		log.Printf("Failed to send message: %v", err)
	}

	time.Sleep(time.Second)

	if err = client.Close(); err != nil {
		log.Printf("Failed to close client: %v", err)
	}
}

func main() {
	flag.Parse()

	go server()
	time.Sleep(100 * time.Millisecond)

	client()
}
