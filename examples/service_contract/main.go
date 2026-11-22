package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/z46-dev/arctic"
	"github.com/z46-dev/arctic/examples/service_contract/proto"
)

var flagAddr *string = flag.String("addr", "localhost:8080", "Address for the server to bind to (e.g., [::1]:8080)")

func server() {
	var (
		server *arctic.ContractServer[proto.Contract]
		err    error
	)

	if server, err = arctic.NewContractServer(arctic.ServerConfig{
		BindAddress: *flagAddr,
		BufferSize:  1024,
		Timeout:     5 * time.Second,
	}, proto.ContractFor()); err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	server.OnClient(func(client *arctic.ContractServerClient[proto.Contract]) {
		var logPrefix string = fmt.Sprintf("[%d | %s]", client.ID(), client.RemoteAddr())

		log.Printf("%s Client connected", logPrefix)

		client.Contract.OnJoin(func(_ *arctic.ContractContext, request proto.InitialRequest) (response proto.GenericResponse) {
			response.OK = true
			return
		})

		client.Contract.OnMath(func(_ *arctic.ContractContext, request proto.MathRequest) (response proto.MathResponse) {
			response.Valid = true

			switch request.Operand {
			case proto.MathOperandAddition:
				response.Result = request.FirstOperator + request.SecondOperator
			case proto.MathOperandSubtraction:
				response.Result = request.FirstOperator - request.SecondOperator
			case proto.MathOperandMultiplication:
				response.Result = request.FirstOperator * request.SecondOperator
			case proto.MathOperandDivision:
				if request.SecondOperator == 0 {
					response.Valid = false
					response.ErrorMessage = "Cannot divide by zero!"
				} else {
					response.Result = request.FirstOperator / request.SecondOperator
				}
			}

			return
		})

		client.Contract.OnDone(func(ctx *arctic.ContractContext, _ struct{}) (response proto.GenericResponse) {
			response.OK = true
			response.Message = "Goodbye!"
			ctx.CloseAfterResponse()
			return
		})

		client.Contract.OnDataStreaming(func(ctx *proto.DataStreamingContext, request proto.DataStreamingRequest) (response proto.GenericResponse) {
			var (
				done                   chan struct{} = make(chan struct{})
				received, sizeReceived int           = 0, 0
			)

			// The request stream can close independently while this handler still replies.
			ctx.OnStreamClose(func() {
				close(done)
			})

			ctx.Stream.OnData(func(_ *arctic.ContractContext, data proto.DataStreamingBody) (reply proto.DataStreamingBody) {
				received++
				sizeReceived += len(data.Chunk)
				reply = data
				return
			})

			<-done
			if err := ctx.CloseStream(); err != nil {
				log.Printf("%s Failed to close stream: %v", logPrefix, err)
			}

			response.OK = request.NumExpectedChunks == received
			response.Message = fmt.Sprintf("Received %d chunks (%d bytes)", received, sizeReceived)
			return
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
		client *arctic.ContractClient[proto.Contract]
		err    error
	)

	if client, err = arctic.NewContractClient(arctic.ClientConfig{
		ServerAddress: *flagAddr,
		BufferSize:    1024,
		Timeout:       5 * time.Second,
	}, proto.ContractFor()); err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	client.OnClose(func() {
		log.Println("Connection closed by server")
	})

	client.OnError(func(err error) {
		log.Printf("Client error: %v", err)
	})

	if err = client.Connect(); err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}

	var (
		resp1, resp3 proto.GenericResponse
		resp2        proto.MathResponse
	)

	if resp1, err = client.Contract.Join(context.Background(), proto.InitialRequest{Message: "Hi"}); err != nil {
		log.Printf("Failed to perform join: %v", err)
	} else if !resp1.OK {
		log.Printf("Failed to join: %v", resp1.Message)
	}

	if resp2, err = client.Contract.Math(context.Background(), proto.MathRequest{
		FirstOperator:  1,
		SecondOperator: 3,
		Operand:        proto.MathOperandAddition,
	}); err != nil {
		log.Printf("Failed to perform math: %v", err)
	} else if !resp2.Valid {
		log.Printf("Failed to compute math: %v", resp2.ErrorMessage)
	} else {
		log.Printf("Math result: %v", resp2.Result)
	}

	// Configure the stream before starting the synchronous call, which waits for its final response.
	var streamContext *proto.DataStreamingContext = client.Contract.NewDataStreamingContext(context.Background())
	streamContext.OnStreamClose(func() {
		log.Println("Remote stream side closed")
	})

	go func() {
		for range 10 {
			time.Sleep(100 * time.Millisecond)
			if _, err := streamContext.Stream.Data(streamContext, proto.DataStreamingBody{Chunk: []byte("chunk")}); err != nil {
				log.Printf("Failed to send stream chunk: %v", err)
				return
			}
		}

		if err := streamContext.CloseStream(); err != nil {
			log.Printf("Failed to close stream: %v", err)
		}
	}()

	if resp3, err = client.Contract.DataStreaming(streamContext, proto.DataStreamingRequest{NumExpectedChunks: 10}); err != nil {
		log.Printf("Failed to perform data streaming: %v", err)
	} else {
		log.Printf("Streaming response: %s", resp3.Message)
	}

	if resp1, err = client.Contract.Done(context.Background(), struct{}{}); err != nil {
		log.Printf("Failed to perform done: %v", err)
	} else if !resp1.OK {
		log.Printf("Failed to done: %v", resp1.Message)
	} else {
		log.Printf("Done response: %v", resp1.Message)
	}

	// Keep the client running to receive messages
	time.Sleep(time.Second)
}

func main() {
	flag.Parse()

	go server()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	client()
}
