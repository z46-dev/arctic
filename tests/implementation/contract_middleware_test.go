package implementation

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/z46-dev/arctic"
	"github.com/z46-dev/arctic/examples/service_contract/proto"
	"github.com/z46-dev/arctic/tests/util"
)

func TestContractMiddleware(t *testing.T) {
	var (
		server         *arctic.ContractServer[proto.Contract]
		client         *arctic.ContractClient[proto.Contract]
		listenDone     chan error
		address        string
		err            error
		handlerInvoked atomic.Int32
	)

	if server, err = arctic.NewContractServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		BufferSize:  1024,
		Timeout:     time.Second,
	}, proto.ContractFor()); err != nil {
		t.Fatalf("new contract server: %v", err)
	}

	server.OnClient(func(client *arctic.ContractServerClient[proto.Contract]) {
		client.Contract.AddInboundMiddleware(func(_ context.Context, message *arctic.ContractMessage) (action arctic.ContractMiddlewareAction, response any, err error) {
			if message.Type != proto.ContractMessageContractJoin || message.Phase != arctic.ContractMessageRequest {
				return
			}

			var request proto.InitialRequest = message.Body.(proto.InitialRequest)
			switch request.Message {
			case "inbound-mutate":
				request.Message = "changed by inbound"
				message.Body = request
			case "inbound-respond":
				action = arctic.ContractMiddlewareRespond
				response = proto.GenericResponse{OK: true, Message: "short circuited"}
			case "inbound-drop":
				action = arctic.ContractMiddlewareDrop
			}

			return
		})

		client.Contract.AddOutboundMiddleware(func(_ context.Context, message *arctic.ContractMessage) (action arctic.ContractMiddlewareAction, _ any, err error) {
			if message.Type == proto.ContractMessageContractJoin && message.Phase == arctic.ContractMessageResponse {
				var response proto.GenericResponse = message.Body.(proto.GenericResponse)
				response.Message = "server outbound: " + response.Message
				message.Body = response
			}

			return
		})

		client.Contract.OnJoin(func(_ *arctic.ContractContext, request proto.InitialRequest) (response proto.GenericResponse) {
			handlerInvoked.Add(1)
			response.OK = true
			response.Message = request.Message
			return
		})
	})

	listenDone = util.StartServer(t, server.Server)
	address = util.WaitForAddress(t, server.Server)
	t.Cleanup(func() {
		util.CloseServer(t, server.Server, listenDone)
	})

	if client, err = arctic.NewContractClient(arctic.ClientConfig{
		ServerAddress: address,
		BufferSize:    1024,
		Timeout:       time.Second,
	}, proto.ContractFor()); err != nil {
		t.Fatalf("new contract client: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Close()
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect contract client: %v", err)
	}

	client.Contract.AddOutboundMiddleware(func(_ context.Context, message *arctic.ContractMessage) (action arctic.ContractMiddlewareAction, response any, err error) {
		if message.Type != proto.ContractMessageContractJoin || message.Phase != arctic.ContractMessageRequest {
			return
		}

		var request proto.InitialRequest = message.Body.(proto.InitialRequest)
		switch request.Message {
		case "outbound-mutate":
			request.Message = "changed by outbound"
			message.Body = request
		case "outbound-respond":
			action = arctic.ContractMiddlewareRespond
			response = proto.GenericResponse{OK: true, Message: "outbound short circuit"}
		case "outbound-drop":
			action = arctic.ContractMiddlewareDrop
		}

		return
	})

	client.Contract.AddInboundMiddleware(func(_ context.Context, message *arctic.ContractMessage) (action arctic.ContractMiddlewareAction, _ any, err error) {
		if message.Type == proto.ContractMessageContractJoin && message.Phase == arctic.ContractMessageResponse {
			var response proto.GenericResponse = message.Body.(proto.GenericResponse)
			response.Message = "client inbound: " + response.Message
			message.Body = response
		}

		return
	})

	var response proto.GenericResponse
	for _, testCase := range []struct {
		request  string
		response string
	}{
		{request: "outbound-mutate", response: "client inbound: server outbound: changed by outbound"},
		{request: "inbound-mutate", response: "client inbound: server outbound: changed by inbound"},
		{request: "inbound-respond", response: "client inbound: server outbound: short circuited"},
		{request: "outbound-respond", response: "outbound short circuit"},
	} {
		if response, err = client.Contract.Join(context.Background(), proto.InitialRequest{Message: testCase.request}); err != nil {
			t.Fatalf("join %q: %v", testCase.request, err)
		} else if !response.OK || response.Message != testCase.response {
			t.Fatalf("join %q returned %+v; want message %q", testCase.request, response, testCase.response)
		}
	}

	var handlerCount int32 = handlerInvoked.Load()
	if _, err = client.Contract.Join(context.Background(), proto.InitialRequest{Message: "inbound-drop"}); !errors.Is(err, arctic.ErrContractMessageDropped) {
		t.Fatalf("expected inbound drop error, got %v", err)
	}

	if _, err = client.Contract.Join(context.Background(), proto.InitialRequest{Message: "outbound-drop"}); !errors.Is(err, arctic.ErrContractMessageDropped) {
		t.Fatalf("expected outbound drop error, got %v", err)
	}

	if handlerInvoked.Load() != handlerCount {
		t.Fatalf("dropped or short-circuited requests reached the handler: before=%d after=%d", handlerCount, handlerInvoked.Load())
	}
}
