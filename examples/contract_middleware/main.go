package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/z46-dev/arctic"
	"github.com/z46-dev/arctic/examples/contract_middleware/proto"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	var server *arctic.ContractServer[proto.Contract]
	if server, err = arctic.NewContractServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		BufferSize:  1024,
		Timeout:     2 * time.Second,
	}, proto.ContractFor()); err != nil {
		return
	}

	server.OnClient(setupServer)

	var listenDone chan error = make(chan error, 1)
	go func() {
		listenDone <- server.Listen()
	}()

	defer func() {
		if closeErr := server.Close(); err == nil {
			err = closeErr
		}

		if listenErr := <-listenDone; err == nil {
			err = listenErr
		}
	}()

	var address string
	if address, err = waitForAddress(server.Server); err != nil {
		return
	}

	var client *arctic.ContractClient[proto.Contract]
	if client, err = arctic.NewContractClient(arctic.ClientConfig{
		ServerAddress: address,
		BufferSize:    1024,
		Timeout:       2 * time.Second,
	}, proto.ContractFor()); err != nil {
		return
	}

	defer func() {
		if closeErr := client.Close(); err == nil {
			err = closeErr
		}
	}()

	setupClient(client.Contract)
	if err = client.Connect(); err != nil {
		return
	}

	for _, message := range []string{
		"normal",
		"server-rewrite",
		"server-reply",
		"client-rewrite",
		"client-reply",
		"server-drop",
		"client-drop",
		"server-close",
	} {
		var response proto.PingResponse
		if response, err = client.Contract.Ping(context.Background(), proto.PingRequest{Message: message}); errors.Is(err, arctic.ErrContractMessageDropped) {
			fmt.Printf("%q was dropped by middleware\n", message)
			err = nil
		} else if err != nil {
			return
		} else {
			fmt.Printf("%q -> %q\n", message, response.Message)
		}
	}

	return
}

func setupServer(client *arctic.ContractServerClient[proto.Contract]) {
	client.Contract.AddInboundMiddleware(func(ctx *arctic.ContractMiddlewareContext, message *arctic.ContractMessage) (action arctic.ContractMiddlewareAction, response any, err error) {
		if message.Type != proto.ContractMessageContractPing || message.Phase != arctic.ContractMessageRequest {
			return
		}

		var request proto.PingRequest = message.Body.(proto.PingRequest)
		switch request.Message {
		case "server-rewrite":
			request.Message = "rewritten inbound"
			message.Body = request
		case "server-reply":
			action = arctic.ContractMiddlewareRespond
			response = proto.PingResponse{Message: "answered inbound"}
		case "server-drop":
			action = arctic.ContractMiddlewareDrop
		case "server-close":
			ctx.CloseAfterResponse()
		}

		return
	})

	client.Contract.AddOutboundMiddleware(func(_ *arctic.ContractMiddlewareContext, message *arctic.ContractMessage) (action arctic.ContractMiddlewareAction, _ any, err error) {
		if message.Type == proto.ContractMessageContractPing && message.Phase == arctic.ContractMessageResponse {
			var response proto.PingResponse = message.Body.(proto.PingResponse)
			response.Message = "server outbound: " + response.Message
			message.Body = response
		}

		return
	})

	client.Contract.OnPing(func(_ *arctic.ContractContext, request proto.PingRequest) (response proto.PingResponse) {
		response.Message = request.Message
		return
	})
}

func setupClient(contract proto.Contract) {
	contract.AddOutboundMiddleware(func(_ *arctic.ContractMiddlewareContext, message *arctic.ContractMessage) (action arctic.ContractMiddlewareAction, response any, err error) {
		if message.Type != proto.ContractMessageContractPing || message.Phase != arctic.ContractMessageRequest {
			return
		}

		var request proto.PingRequest = message.Body.(proto.PingRequest)
		switch request.Message {
		case "client-rewrite":
			request.Message = "rewritten outbound"
			message.Body = request
		case "client-reply":
			action = arctic.ContractMiddlewareRespond
			response = proto.PingResponse{Message: "answered locally"}
		case "client-drop":
			action = arctic.ContractMiddlewareDrop
		}

		return
	})

	contract.AddInboundMiddleware(func(_ *arctic.ContractMiddlewareContext, message *arctic.ContractMessage) (action arctic.ContractMiddlewareAction, _ any, err error) {
		if message.Type == proto.ContractMessageContractPing && message.Phase == arctic.ContractMessageResponse {
			var response proto.PingResponse = message.Body.(proto.PingResponse)
			response.Message = "client inbound: " + response.Message
			message.Body = response
		}

		return
	})
}

func waitForAddress(server *arctic.Server) (address string, err error) {
	var (
		timer  *time.Timer  = time.NewTimer(5 * time.Second)
		ticker *time.Ticker = time.NewTicker(time.Millisecond)
	)

	defer timer.Stop()
	defer ticker.Stop()

	for {
		select {
		case <-timer.C:
			err = errors.New("arctic: timed out waiting for server address")
			return
		case <-ticker.C:
			var addr net.Addr = server.Addr()
			if addr != nil {
				address = addr.String()
				return
			}
		}
	}
}
