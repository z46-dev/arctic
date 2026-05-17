package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/z46-dev/arctic"
)

var flagAddr *string = flag.String("addr", "[::]:8080", "Address for the server to bind to (e.g., [::1]:8080)")

type Message struct {
	Text   string
	Number int
}

func server() {
	var (
		server *arctic.GobServer[Message]
		err    error
	)

	if server, err = arctic.NewGobServer[Message](arctic.ServerConfig{
		BindAddress: *flagAddr,
		BufferSize:  1024,
		Timeout:     5 * time.Second,
	}); err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	server.OnClient(func(client *arctic.GobServerClient[Message]) {
		var logPrefix string = fmt.Sprintf("[%d | %s]", client.ID(), client.RemoteAddr())

		log.Printf("%s Client connected", logPrefix)

		client.OnMessage(func(msg Message) {
			log.Printf("%s Received message: %+v", logPrefix, msg)
			client.Send(Message{
				Text:   "Hello, Client!",
				Number: msg.Number + 1,
			})
		})

		client.OnClose(func() {
			log.Printf("%s Client disconnected", logPrefix)
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
		client *arctic.GobClient[Message]
		err    error
	)

	if client, err = arctic.NewGobClient[Message](arctic.ClientConfig{
		ServerAddress: *flagAddr,
		BufferSize:    1024,
		Timeout:       5 * time.Second,
	}); err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	client.OnMessage(func(msg Message) {
		log.Printf("Received message from server: %+v", msg)
	})

	client.OnClose(func() {
		log.Println("Connection closed by server")
	})

	client.OnError(func(err error) {
		log.Printf("Client error: %v", err)
	})

	if err = client.Connect(); err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}

	// Send a message to the server
	if err = client.Send(Message{
		Text:   "Hello, Server!",
		Number: 42,
	}); err != nil {
		log.Printf("Failed to send message: %v", err)
	}

	// Keep the client running to receive messages
	time.Sleep(time.Second)
}

func main() {
	flag.Parse()

	var err error
	if err = arctic.RegisterGobType(Message{}); err != nil {
		log.Fatalf("Failed to register Gob type: %v", err)
	}

	go server()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	client()
}
