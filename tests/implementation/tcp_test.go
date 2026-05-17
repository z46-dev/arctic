package implementation

import (
	"errors"
	"testing"
	"time"

	"github.com/z46-dev/arctic"
	"github.com/z46-dev/arctic/tests/util"
)

type (
	unregisteredGobMessage struct {
		Text string
	}
)

func TestTCPClientServerRoundTrip(t *testing.T) {
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
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatalf("new server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.ServerClient) {
		if client.ID() == 0 {
			asyncErrors <- errors.New("expected server client id to be set")
			return
		}

		client.Use(func(context *arctic.MessageContext, next arctic.Next) (err error) {
			context.Message = append([]byte("seen:"), context.Message...)
			err = next(context)
			return
		})

		client.OnMessage(func(message []byte) {
			var response []byte = append([]byte("echo:"), message...)
			var err error

			if err = client.Send(response); err != nil {
				asyncErrors <- err
			}
		})
	})

	listenDone = util.StartServer(t, server)
	address = util.WaitForAddress(t, server)

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: address,
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
		t.Fatalf("send message: %v", err)
	}

	select {
	case message := <-received:
		var expected string = "echo:seen:ping"

		if string(message) != expected {
			t.Fatalf("expected %q, got %q", expected, string(message))
		}
	case err = <-asyncErrors:
		t.Fatalf("async error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for echo")
	}

	if err = client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	util.CloseServer(t, server, listenDone)
	util.AssertNoAsyncError(t, asyncErrors)
}

func TestTCPUnsafeZeroCopyRoundTrip(t *testing.T) {
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
		BindAddress:    "127.0.0.1:0",
		BufferSize:     1024,
		Timeout:        time.Second,
		UnsafeZeroCopy: true,
	}); err != nil {
		t.Fatalf("new server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.ServerClient) {
		client.OnMessage(func(message []byte) {
			var response []byte = append([]byte("zero:"), message...)
			var err error

			if err = client.Send(response); err != nil {
				asyncErrors <- err
			}
		})
	})

	listenDone = util.StartServer(t, server)
	address = util.WaitForAddress(t, server)

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress:  address,
		BufferSize:     1024,
		Timeout:        time.Second,
		UnsafeZeroCopy: true,
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
		t.Fatalf("send message: %v", err)
	}

	util.AssertMessage(t, received, "zero:ping")

	if err = client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}

	util.CloseServer(t, server, listenDone)
	util.AssertNoAsyncError(t, asyncErrors)
}

func TestGobClientServerRoundTrip(t *testing.T) {
	var (
		server      *arctic.GobServer[util.GobMessage]
		client      *arctic.GobClient[util.GobMessage]
		err         error
		address     string
		listenDone  chan error
		asyncErrors chan error               = make(chan error, 8)
		received    chan util.GobMessage = make(chan util.GobMessage, 1)
	)

	if err = arctic.RegisterGobType(util.GobMessage{}); err != nil {
		t.Fatalf("register gob type: %v", err)
	}

	if server, err = arctic.NewGobServer[util.GobMessage](arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatalf("new gob server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.GobServerClient[util.GobMessage]) {
		client.OnMessage(func(message util.GobMessage) {
			var response util.GobMessage = util.GobMessage{
				Text:  "echo:" + message.Text,
				Count: message.Count + 1,
			}
			var err error

			if err = client.Send(response); err != nil {
				asyncErrors <- err
			}
		})
	})

	listenDone = util.StartGobServer(t, server)
	address = util.WaitForAddress(t, server)

	if client, err = arctic.NewGobClient[util.GobMessage](arctic.ClientConfig{
		ServerAddress: address,
		BufferSize:    1024,
		Timeout:       time.Second,
	}); err != nil {
		t.Fatalf("new gob client: %v", err)
	}

	client.OnError(func(err error) {
		asyncErrors <- err
	})

	client.OnMessage(func(message util.GobMessage) {
		received <- message
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect gob client: %v", err)
	}

	if err = client.Send(util.GobMessage{Text: "ping", Count: 41}); err != nil {
		t.Fatalf("send gob message: %v", err)
	}

	select {
	case message := <-received:
		var expected util.GobMessage = util.GobMessage{
			Text:  "echo:ping",
			Count: 42,
		}

		if message != expected {
			t.Fatalf("expected %#v, got %#v", expected, message)
		}
	case err = <-asyncErrors:
		t.Fatalf("async error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for gob echo")
	}

	if err = client.Close(); err != nil {
		t.Fatalf("close gob client: %v", err)
	}

	util.CloseGobServer(t, server, listenDone)
	util.AssertNoAsyncError(t, asyncErrors)
}

func TestGobConstructorsRequireRegistration(t *testing.T) {
	var err error

	_, err = arctic.NewGobClient[unregisteredGobMessage](arctic.ClientConfig{
		ServerAddress: "127.0.0.1:1",
	})

	if !errors.Is(err, arctic.ErrGobTypeNotRegistered) {
		t.Fatalf("expected ErrGobTypeNotRegistered for client, got %v", err)
	}

	_, err = arctic.NewGobServer[unregisteredGobMessage](arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
	})

	if !errors.Is(err, arctic.ErrGobTypeNotRegistered) {
		t.Fatalf("expected ErrGobTypeNotRegistered for server, got %v", err)
	}
}
