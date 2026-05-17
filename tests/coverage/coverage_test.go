package coverage

import (
	"errors"
	"testing"
	"time"

	"github.com/z46-dev/arctic"
	"github.com/z46-dev/arctic/tests/util"
)

type (
	coverageUnregisteredMessage struct {
		Text string
	}
)

func TestConstructorsAndValidationForCoverage(t *testing.T) {
	var (
		client *arctic.Client
		server *arctic.Server
		err    error
	)

	if client, err = arctic.NewClient(arctic.ClientConfig{}); err != nil {
		t.Fatalf("new default client: %v", err)
	}

	if client.RemoteAddr() != nil {
		t.Fatal("expected unconnected client remote address to be nil")
	}

	if client.LocalAddr() != nil {
		t.Fatal("expected unconnected client local address to be nil")
	}

	if err = client.Close(); err != nil {
		t.Fatalf("close unconnected client: %v", err)
	}

	if server, err = arctic.NewServer(arctic.ServerConfig{}); err != nil {
		t.Fatalf("new default server: %v", err)
	}

	if server.Addr() != nil {
		t.Fatal("expected stopped server address to be nil")
	}

	if err = server.Close(); err != nil {
		t.Fatalf("close stopped server: %v", err)
	}

	_, err = arctic.NewClient(arctic.ClientConfig{Protocol: arctic.Protocol("bad")})

	if !errors.Is(err, arctic.ErrProtocolUnsupported) {
		t.Fatalf("expected ErrProtocolUnsupported for client, got %v", err)
	}

	_, err = arctic.NewServer(arctic.ServerConfig{Protocol: arctic.Protocol("bad")})

	if !errors.Is(err, arctic.ErrProtocolUnsupported) {
		t.Fatalf("expected ErrProtocolUnsupported for server, got %v", err)
	}
}

func TestGobRegistryAndConstructorsForCoverage(t *testing.T) {
	var err error

	_, err = arctic.NewGobClient[coverageUnregisteredMessage](arctic.ClientConfig{})

	if !errors.Is(err, arctic.ErrGobTypeNotRegistered) {
		t.Fatalf("expected ErrGobTypeNotRegistered for gob client, got %v", err)
	}

	if err = arctic.RegisterGobType(42); !errors.Is(err, arctic.ErrGobTypeInvalid) {
		t.Fatalf("expected ErrGobTypeInvalid, got %v", err)
	}

	if err = arctic.RegisterGobType(util.GobMessage{}); err != nil {
		t.Fatalf("register gob message: %v", err)
	}

	if !arctic.IsGobTypeRegistered[util.GobMessage]() {
		t.Fatal("expected gob message type to be registered")
	}

	if _, err = arctic.NewGobClient[util.GobMessage](arctic.ClientConfig{}); err != nil {
		t.Fatalf("new registered gob client: %v", err)
	}

	if _, err = arctic.NewGobServer[util.GobMessage](arctic.ServerConfig{}); err != nil {
		t.Fatalf("new registered gob server: %v", err)
	}
}

func TestRawRoundTripsForCoverage(t *testing.T) {
	var protocols []arctic.Protocol = []arctic.Protocol{
		arctic.ProtocolTCP,
		arctic.ProtocolUDP,
	}

	for _, protocol := range protocols {
		var protocol arctic.Protocol = protocol

		t.Run(string(protocol), func(t *testing.T) {
			runRawRoundTripForCoverage(t, protocol)
		})
	}
}

func runRawRoundTripForCoverage(t *testing.T, protocol arctic.Protocol) {
	var (
		server      *arctic.Server
		client      *arctic.Client
		err         error
		address     string
		listenDone  chan error
		asyncErrors chan error  = make(chan error, 8)
		received    chan []byte = make(chan []byte, 1)
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    protocol,
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatalf("new server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.ServerClient) {
		client.OnMessage(func(message []byte) {
			var err error

			if err = client.Send(append([]byte("coverage:"), message...)); err != nil {
				asyncErrors <- err
			}
		})
	})

	listenDone = util.StartServer(t, server)
	address = util.WaitForAddress(t, server)

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: address,
		Protocol:      protocol,
		BufferSize:    1024,
		Timeout:       time.Second,
	}); err != nil {
		t.Fatalf("new client: %v", err)
	}

	client.OnError(func(err error) {
		asyncErrors <- err
	})

	client.OnMessage(func(message []byte) {
		received <- append([]byte{}, message...)
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect client: %v", err)
	}

	if err = client.Send([]byte("ping")); err != nil {
		t.Fatalf("send: %v", err)
	}

	util.AssertMessage(t, received, "coverage:ping")
	util.CloseClient(t, client)
	util.CloseServer(t, server, listenDone)
	util.AssertNoAsyncError(t, asyncErrors)
}
