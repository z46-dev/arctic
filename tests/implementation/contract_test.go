package implementation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/z46-dev/arctic"
	"github.com/z46-dev/arctic/examples/service_contract/proto"
	"github.com/z46-dev/arctic/tests/implementation/recursive_contract"
	"github.com/z46-dev/arctic/tests/util"
)

func TestContractDuplexStreaming(t *testing.T) {
	var (
		server                        *arctic.ContractServer[proto.Contract]
		client                        *arctic.ContractClient[proto.Contract]
		listenDone                    chan error
		address                       string
		err                           error
		cancelStarted, cancelObserved chan struct{} = make(chan struct{}, 1), make(chan struct{}, 1)
	)

	if server, err = arctic.NewContractServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		BufferSize:  1024,
		Timeout:     time.Second,
	}, proto.ContractFor()); err != nil {
		t.Fatalf("new contract server: %v", err)
	}

	server.OnClient(func(client *arctic.ContractServerClient[proto.Contract]) {
		client.Contract.OnDataStreaming(func(ctx *proto.DataStreamingContext, request proto.DataStreamingRequest) (response proto.GenericResponse) {
			if request.NumExpectedChunks < 0 {
				cancelStarted <- struct{}{}
				<-ctx.Done()
				cancelObserved <- struct{}{}
				return
			}

			var (
				finished chan struct{} = make(chan struct{})
				received int
			)

			ctx.OnStreamClose(func() {
				close(finished)
			})

			ctx.Stream.OnData(func(_ *arctic.ContractContext, message proto.DataStreamingBody) (reply proto.DataStreamingBody) {
				received++
				reply = message
				return
			})

			select {
			case <-finished:
			case <-ctx.Done():
				t.Errorf("stream context canceled before half-close: %v", ctx.Err())
			}

			if err := ctx.CloseStream(); err != nil {
				t.Errorf("close server stream: %v", err)
			}

			response.OK = received == request.NumExpectedChunks
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

	t.Cleanup(func() { _ = client.Close() })
	if err = client.Connect(); err != nil {
		t.Fatalf("connect contract client: %v", err)
	}

	var (
		streamContext  *proto.DataStreamingContext = client.Contract.NewDataStreamingContext(context.Background())
		chunks         [][]byte                    = [][]byte{[]byte("one"), []byte("two"), []byte("three")}
		streamResponse chan proto.GenericResponse  = make(chan proto.GenericResponse, 1)
		streamError    chan error                  = make(chan error, 1)
	)

	go func() {
		var (
			response proto.GenericResponse
			callErr  error
		)

		response, callErr = client.Contract.DataStreaming(streamContext, proto.DataStreamingRequest{NumExpectedChunks: len(chunks)})
		streamResponse <- response
		streamError <- callErr
	}()

	for _, chunk := range chunks {
		var reply proto.DataStreamingBody
		if reply, err = streamContext.Stream.Data(streamContext, proto.DataStreamingBody{Chunk: chunk}); err != nil {
			t.Fatalf("stream data call: %v", err)
		} else if string(reply.Chunk) != string(chunk) {
			t.Fatalf("expected echoed chunk %q, got %q", chunk, reply.Chunk)
		}
	}

	if err = streamContext.CloseStream(); err != nil {
		t.Fatalf("close client stream: %v", err)
	}

	var response proto.GenericResponse
	if err = <-streamError; err != nil {
		t.Fatalf("data streaming call: %v", err)
	} else {
		response = <-streamResponse
	}

	if !response.OK {
		t.Fatal("expected final response to confirm all stream messages")
	}

	var (
		callContext context.Context
		cancel      context.CancelFunc
		cancelCall  chan error = make(chan error, 1)
	)

	callContext, cancel = context.WithCancel(context.Background())
	var cancelStreamContext *proto.DataStreamingContext = client.Contract.NewDataStreamingContext(callContext)
	go func() {
		var callErr error
		_, callErr = client.Contract.DataStreaming(cancelStreamContext, proto.DataStreamingRequest{NumExpectedChunks: -1})
		cancelCall <- callErr
	}()

	select {
	case <-cancelStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for streaming handler to start")
	}

	cancel()
	select {
	case err = <-cancelCall:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for canceled streaming call")
	}

	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remote stream cancellation")
	}
}

// TestNestedContractStreams verifies generated contract routing across three nested stream scopes.
func TestNestedContractStreams(t *testing.T) {
	var (
		server     *arctic.ContractServer[recursive_contract.RootContract]
		client     *arctic.ContractClient[recursive_contract.RootContract]
		listenDone chan error
		address    string
		err        error
	)

	if server, err = arctic.NewContractServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		BufferSize:  1024,
		Timeout:     time.Second,
	}, recursive_contract.RootContractFor()); err != nil {
		t.Fatalf("new contract server: %v", err)
	}

	server.OnClient(func(client *arctic.ContractServerClient[recursive_contract.RootContract]) {
		client.Contract.OnOpenRoot(func(ctx *recursive_contract.OpenRootContext, _ recursive_contract.Request) (response recursive_contract.Response) {
			var (
				nestedContext *recursive_contract.OpenNestedContext = ctx.Stream.NewOpenNestedContext(ctx)
				callErr       error
			)

			nestedContext.Stream.OnOpenDeep(func(deepContext *recursive_contract.OpenDeepContext, _ recursive_contract.Request) (response recursive_contract.Response) {
				var leafContext *recursive_contract.OpenLeafContext = deepContext.Stream.NewOpenLeafContext(deepContext)
				leafContext.Stream.OnLeaf(func(_ *arctic.ContractContext, message recursive_contract.Message) (response recursive_contract.Response) {
					response.Value = message.Value
					return
				})

				if response, callErr = deepContext.Stream.OpenLeaf(leafContext, recursive_contract.Request{}); callErr != nil {
					t.Errorf("deep stream call: %v", callErr)
				}

				return
			})

			if response, callErr = ctx.Stream.OpenNested(nestedContext, recursive_contract.Request{}); callErr != nil {
				t.Errorf("nested stream call: %v", callErr)
			}

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
	}, recursive_contract.RootContractFor()); err != nil {
		t.Fatalf("new contract client: %v", err)
	}

	t.Cleanup(func() {
		_ = client.Close()
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect contract client: %v", err)
	}

	var streamContext *recursive_contract.OpenRootContext = client.Contract.NewOpenRootContext(context.Background())
	streamContext.Stream.OnOpenNested(func(nestedContext *recursive_contract.OpenNestedContext, _ recursive_contract.Request) (response recursive_contract.Response) {
		var (
			deepContext *recursive_contract.OpenDeepContext = nestedContext.Stream.NewOpenDeepContext(nestedContext)
			callErr     error
		)

		deepContext.Stream.OnOpenLeaf(func(leafContext *recursive_contract.OpenLeafContext, _ recursive_contract.Request) (response recursive_contract.Response) {
			if response, callErr = leafContext.Stream.Leaf(leafContext, recursive_contract.Message{Value: "reached deepest contract"}); callErr != nil {
				t.Errorf("deepest unary call: %v", callErr)
			}

			return
		})

		if response, callErr = nestedContext.Stream.OpenDeep(deepContext, recursive_contract.Request{}); callErr != nil {
			t.Errorf("deep nested stream call: %v", callErr)
		}

		return
	})

	var response recursive_contract.Response
	if response, err = client.Contract.OpenRoot(streamContext, recursive_contract.Request{}); err != nil {
		t.Fatalf("root stream call: %v", err)
	} else if response.Value != "reached deepest contract" {
		t.Fatalf("unexpected deepest response: %+v", response)
	}
}
