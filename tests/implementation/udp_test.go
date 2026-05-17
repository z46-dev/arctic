package implementation

import (
	"errors"
	"testing"
	"time"

	"github.com/z46-dev/arctic"
	"github.com/z46-dev/arctic/tests/util"
)

func TestUDPClientServerRoundTrip(t *testing.T) {
	var (
		server      *arctic.Server
		client      *arctic.Client
		err         error
		address     string
		listenDone  chan error
		asyncErrors chan error  = make(chan error, 8)
		connected   chan int    = make(chan int, 1)
		received    chan []byte = make(chan []byte, 1)
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    arctic.ProtocolUDP,
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatalf("new udp server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.ServerClient) {
		connected <- client.ID()

		client.OnMessage(func(message []byte) {
			var response []byte = append([]byte("udp:"), message...)
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
		Protocol:      arctic.ProtocolUDP,
		BufferSize:    1024,
		Timeout:       time.Second,
	}); err != nil {
		t.Fatalf("new udp client: %v", err)
	}

	client.OnError(func(err error) {
		asyncErrors <- err
	})

	client.OnMessage(func(message []byte) {
		received <- append([]byte{}, message...)
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect udp client: %v", err)
	}

	if err = client.Send([]byte("ping")); err != nil {
		t.Fatalf("send udp message: %v", err)
	}

	select {
	case id := <-connected:
		if id == 0 {
			t.Fatal("expected udp server client id to be set")
		}
	case err = <-asyncErrors:
		t.Fatalf("async error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for udp client registration")
	}

	select {
	case message := <-received:
		var expected string = "udp:ping"

		if string(message) != expected {
			t.Fatalf("expected %q, got %q", expected, string(message))
		}
	case err = <-asyncErrors:
		t.Fatalf("async error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for udp response")
	}

	if err = client.Close(); err != nil {
		t.Fatalf("close udp client: %v", err)
	}

	util.CloseServer(t, server, listenDone)
	util.AssertNoAsyncError(t, asyncErrors)
}

func TestUDPClientMetadataAvailableOnClient(t *testing.T) {
	var (
		server           *arctic.Server
		client           *arctic.Client
		err              error
		address          string
		listenDone       chan error
		asyncErrors      chan error          = make(chan error, 8)
		receivedMetadata chan map[string]any = make(chan map[string]any, 1)
		received         chan []byte         = make(chan []byte, 1)
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    arctic.ProtocolUDP,
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatalf("new udp server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.ServerClient) {
		receivedMetadata <- client.Metadata()

		client.OnMessage(func(message []byte) {
			var err error

			if err = client.Send(append([]byte("udp-meta:"), message...)); err != nil {
				asyncErrors <- err
			}
		})
	})

	listenDone = util.StartServer(t, server)
	address = util.WaitForAddress(t, server)

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: address,
		Protocol:      arctic.ProtocolUDP,
		BufferSize:    1024,
		Timeout:       time.Second,
		Metadata: map[string]any{
			"tenant":  "acme",
			"attempt": 7,
			"debug":   true,
		},
	}); err != nil {
		t.Fatalf("new udp client: %v", err)
	}

	client.OnError(func(err error) {
		asyncErrors <- err
	})

	client.OnMessage(func(message []byte) {
		received <- append([]byte{}, message...)
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect udp client: %v", err)
	}

	assertMetadata(t, receivedMetadata)

	if err = client.Send([]byte("ping")); err != nil {
		t.Fatalf("send udp message: %v", err)
	}

	util.AssertMessage(t, received, "udp-meta:ping")

	if err = client.Close(); err != nil {
		t.Fatalf("close udp client: %v", err)
	}

	util.CloseServer(t, server, listenDone)
	util.AssertNoAsyncError(t, asyncErrors)
}

func TestUDPUnsafeZeroCopyRoundTrip(t *testing.T) {
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
		Protocol:       arctic.ProtocolUDP,
		BufferSize:     1024,
		Timeout:        time.Second,
		UnsafeZeroCopy: true,
	}); err != nil {
		t.Fatalf("new udp server: %v", err)
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
		Protocol:       arctic.ProtocolUDP,
		BufferSize:     1024,
		Timeout:        time.Second,
		UnsafeZeroCopy: true,
	}); err != nil {
		t.Fatalf("new udp client: %v", err)
	}

	client.OnError(func(err error) {
		asyncErrors <- err
	})

	client.OnMessage(func(message []byte) {
		received <- append([]byte{}, message...)
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect udp client: %v", err)
	}

	if err = client.Send([]byte("ping")); err != nil {
		t.Fatalf("send udp message: %v", err)
	}

	util.AssertMessage(t, received, "zero:ping")

	if err = client.Close(); err != nil {
		t.Fatalf("close udp client: %v", err)
	}

	util.CloseServer(t, server, listenDone)
	util.AssertNoAsyncError(t, asyncErrors)
}

func TestUDPMultipleClientsReceiveOwnReplies(t *testing.T) {
	var (
		server      *arctic.Server
		err         error
		address     string
		listenDone  chan error
		asyncErrors chan error = make(chan error, 8)
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    arctic.ProtocolUDP,
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatalf("new udp server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.ServerClient) {
		client.OnMessage(func(message []byte) {
			var response []byte = append([]byte("reply:"), message...)
			var err error

			if err = client.Send(response); err != nil {
				asyncErrors <- err
			}
		})
	})

	listenDone = util.StartServer(t, server)
	address = util.WaitForAddress(t, server)

	var firstClient *arctic.Client
	var firstMessages chan []byte
	firstClient, firstMessages = util.NewUDPClient(t, address, asyncErrors)
	defer util.CloseClient(t, firstClient)

	var secondClient *arctic.Client
	var secondMessages chan []byte
	secondClient, secondMessages = util.NewUDPClient(t, address, asyncErrors)
	defer util.CloseClient(t, secondClient)

	if err = firstClient.Send([]byte("first")); err != nil {
		t.Fatalf("first send: %v", err)
	}

	if err = secondClient.Send([]byte("second")); err != nil {
		t.Fatalf("second send: %v", err)
	}

	util.AssertMessage(t, firstMessages, "reply:first")
	util.AssertMessage(t, secondMessages, "reply:second")

	util.CloseServer(t, server, listenDone)
	util.AssertNoAsyncError(t, asyncErrors)
}

func TestUDPRejectsOversizedDatagrams(t *testing.T) {
	var (
		server      *arctic.Server
		client      *arctic.Client
		err         error
		address     string
		listenDone  chan error
		asyncErrors chan error = make(chan error, 8)
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    arctic.ProtocolUDP,
		BufferSize:  4,
	}); err != nil {
		t.Fatalf("new udp server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	listenDone = util.StartServer(t, server)
	address = util.WaitForAddress(t, server)

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: address,
		Protocol:      arctic.ProtocolUDP,
		BufferSize:    8,
	}); err != nil {
		t.Fatalf("new udp client: %v", err)
	}

	if err = client.Connect(); err != nil {
		t.Fatalf("connect udp client: %v", err)
	}

	if err = client.Send([]byte("12345")); err != nil {
		t.Fatalf("send oversized udp datagram to server: %v", err)
	}

	select {
	case err = <-asyncErrors:
		if !errors.Is(err, arctic.ErrMessageTooLarge) {
			t.Fatalf("expected ErrMessageTooLarge, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for oversized datagram error")
	}

	if err = client.Close(); err != nil {
		t.Fatalf("close udp client: %v", err)
	}

	util.CloseServer(t, server, listenDone)
}

func TestGobUDPClientServerRoundTrip(t *testing.T) {
	var (
		server      *arctic.GobServer[util.GobMessage]
		client      *arctic.GobClient[util.GobMessage]
		err         error
		address     string
		listenDone  chan error
		asyncErrors chan error           = make(chan error, 8)
		received    chan util.GobMessage = make(chan util.GobMessage, 1)
	)

	if err = arctic.RegisterGobType(util.GobMessage{}); err != nil {
		t.Fatalf("register gob type: %v", err)
	}

	if server, err = arctic.NewGobServer[util.GobMessage](arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    arctic.ProtocolUDP,
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatalf("new gob udp server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.GobServerClient[util.GobMessage]) {
		client.OnMessage(func(message util.GobMessage) {
			var response util.GobMessage = util.GobMessage{
				Text:  "udp:" + message.Text,
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
		Protocol:      arctic.ProtocolUDP,
		BufferSize:    1024,
		Timeout:       time.Second,
	}); err != nil {
		t.Fatalf("new gob udp client: %v", err)
	}

	client.OnError(func(err error) {
		asyncErrors <- err
	})

	client.OnMessage(func(message util.GobMessage) {
		received <- message
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect gob udp client: %v", err)
	}

	if err = client.Send(util.GobMessage{Text: "ping", Count: 41}); err != nil {
		t.Fatalf("send gob udp message: %v", err)
	}

	select {
	case message := <-received:
		var expected util.GobMessage = util.GobMessage{
			Text:  "udp:ping",
			Count: 42,
		}

		if message != expected {
			t.Fatalf("expected %#v, got %#v", expected, message)
		}
	case err = <-asyncErrors:
		t.Fatalf("async error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for gob udp response")
	}

	if err = client.Close(); err != nil {
		t.Fatalf("close gob udp client: %v", err)
	}

	util.CloseGobServer(t, server, listenDone)
	util.AssertNoAsyncError(t, asyncErrors)
}

func TestUDPOnCloseForExplicitAndServerShutdown(t *testing.T) {
	var (
		server        *arctic.Server
		client        *arctic.Client
		err           error
		address       string
		listenDone    chan error
		clientClosed  chan struct{} = make(chan struct{}, 1)
		virtualReady  chan struct{} = make(chan struct{}, 1)
		virtualClosed chan struct{} = make(chan struct{}, 1)
		asyncErrors   chan error    = make(chan error, 8)
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    arctic.ProtocolUDP,
		BufferSize:  1024,
	}); err != nil {
		t.Fatalf("new udp server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.ServerClient) {
		virtualReady <- struct{}{}

		client.OnClose(func() {
			virtualClosed <- struct{}{}
		})

		client.OnMessage(func(message []byte) {})
	})

	listenDone = util.StartServer(t, server)
	address = util.WaitForAddress(t, server)

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: address,
		Protocol:      arctic.ProtocolUDP,
		BufferSize:    1024,
	}); err != nil {
		t.Fatalf("new udp client: %v", err)
	}

	client.OnClose(func() {
		clientClosed <- struct{}{}
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect udp client: %v", err)
	}

	if err = client.Send([]byte("create-virtual-client")); err != nil {
		t.Fatalf("send udp message: %v", err)
	}

	util.AssertClosed(t, virtualReady, "virtual UDP client creation")

	if err = client.Close(); err != nil {
		t.Fatalf("close udp client: %v", err)
	}

	util.AssertClosed(t, clientClosed, "client OnClose")

	util.CloseServer(t, server, listenDone)
	util.AssertClosed(t, virtualClosed, "virtual UDP client OnClose")
	util.AssertNoAsyncError(t, asyncErrors)
}
