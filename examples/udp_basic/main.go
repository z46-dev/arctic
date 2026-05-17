package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/z46-dev/arctic"
)

var flagAddr *string = flag.String("addr", "127.0.0.1:8081", "Address for the UDP server to bind to")

func server() {
	var (
		server *arctic.Server
		err    error
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: *flagAddr,
		Protocol:    arctic.ProtocolUDP,
		BufferSize:  1024,
		Timeout:     5 * time.Second,
	}); err != nil {
		log.Fatalf("Failed to create UDP server: %v", err)
	}

	server.OnClient(func(client *arctic.ServerClient) {
		var logPrefix string = fmt.Sprintf("[%d | %s]", client.ID(), client.RemoteAddr())

		log.Printf("%s UDP client seen", logPrefix)

		client.OnMessage(func(msg []byte) {
			var err error

			log.Printf("%s Received datagram: %s", logPrefix, string(msg))

			if err = client.Send([]byte("UDP message received")); err != nil {
				log.Printf("%s Failed to send response: %v", logPrefix, err)
			}
		})

		client.OnClose(func() {
			log.Printf("%s UDP virtual client closed", logPrefix)
		})

		client.OnError(func(err error) {
			log.Printf("%s UDP client error: %v", logPrefix, err)
		})
	})

	if err = server.Listen(); err != nil {
		log.Fatalf("Failed to start UDP server: %v", err)
	}
}

func client() {
	var (
		client *arctic.Client
		err    error
	)

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: *flagAddr,
		Protocol:      arctic.ProtocolUDP,
		BufferSize:    1024,
		Timeout:       5 * time.Second,
	}); err != nil {
		log.Fatalf("Failed to create UDP client: %v", err)
	}

	client.OnMessage(func(msg []byte) {
		log.Printf("Received datagram from server: %s", string(msg))
	})

	client.OnClose(func() {
		log.Println("UDP client closed")
	})

	client.OnError(func(err error) {
		log.Printf("UDP client error: %v", err)
	})

	if err = client.Connect(); err != nil {
		log.Fatalf("Failed to connect UDP client: %v", err)
	}

	if err = client.Send([]byte("Hello, UDP Server!")); err != nil {
		log.Printf("Failed to send UDP datagram: %v", err)
	}

	time.Sleep(time.Second)

	if err = client.Close(); err != nil {
		log.Printf("Failed to close UDP client: %v", err)
	}
}

func main() {
	flag.Parse()

	go server()
	time.Sleep(100 * time.Millisecond)

	client()
}
