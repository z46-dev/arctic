package implementation_test

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/z46-dev/arctic"
)

type (
	webSocketGobMessage struct {
		Text string
	}
)

func TestWebSocketSidecarUsesServerClientAPI(t *testing.T) {
	var (
		server       *arctic.Server
		sidecar      *arctic.WebSocketServer
		client       *arctic.Client
		received     chan string = make(chan string, 1)
		metadata     chan string = make(chan string, 1)
		listenErrors chan error  = make(chan error, 1)
		replies      chan string = make(chan string, 1)
		address      string
		err          error
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    arctic.ProtocolTCP,
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if sidecar, err = arctic.NewWebSocketServer(arctic.WebSocketServerConfig{
		BindAddress: "127.0.0.1:0",
		Path:        "/arctic",
		Timeout:     time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if err = server.AddWebSocketSidecar(sidecar); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })

	server.OnClient(func(client *arctic.ServerClient) {
		metadata <- fmt.Sprint(client.Metadata()["source"])
		client.OnMessage(func(message []byte) {
			received <- string(message)
			_ = client.Send([]byte("reply:" + string(message)))
		})
	})

	go func() { listenErrors <- server.Listen() }()
	address = waitForWebSocketAddress(t, sidecar)

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: "ws://" + address + "/arctic",
		Protocol:      arctic.ProtocolWebSocket,
		BufferSize:    1024,
		Timeout:       time.Second,
		Metadata:      map[string]any{"source": "websocket"},
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	client.OnMessage(func(message []byte) { replies <- string(message) })
	if err = client.Connect(); err != nil {
		t.Fatal(err)
	}
	if err = client.Send([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	assertString(t, metadata, "websocket")
	assertString(t, received, "hello")
	assertString(t, replies, "reply:hello")
	if err = server.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-listenErrors:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server Listen did not return after Close")
	}
}

func TestListenReturnsEndpointInUse(t *testing.T) {
	var (
		listener net.Listener
		server   *arctic.Server
		address  string
		err      error
	)

	if listener, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	address = listener.Addr().String()
	defer listener.Close()

	if server, err = arctic.NewServer(arctic.ServerConfig{BindAddress: address}); err != nil {
		t.Fatal(err)
	}
	if err = server.Listen(); !errors.Is(err, arctic.ErrEndpointInUse) {
		t.Fatalf("expected ErrEndpointInUse, got %v", err)
	}
}

func TestWebSocketSidecarWorksWithUDPPrimary(t *testing.T) {
	var (
		server  *arctic.Server
		sidecar *arctic.WebSocketServer
		client  *arctic.Client
		replies chan string = make(chan string, 1)
		err     error
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    arctic.ProtocolUDP,
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if sidecar, err = arctic.NewWebSocketServer(arctic.WebSocketServerConfig{BindAddress: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	if err = server.AddWebSocketSidecar(sidecar); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	server.OnClient(func(client *arctic.ServerClient) {
		client.OnMessage(func(message []byte) { _ = client.Send(append([]byte("udp-sidecar:"), message...)) })
	})
	go func() { _ = server.Listen() }()

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: "ws://" + waitForWebSocketAddress(t, sidecar) + "/arctic",
		Protocol:      arctic.ProtocolWebSocket,
		BufferSize:    1024,
		Timeout:       time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	client.OnMessage(func(message []byte) { replies <- string(message) })
	if err = client.Connect(); err != nil {
		t.Fatal(err)
	}
	if err = client.Send([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	assertString(t, replies, "udp-sidecar:hello")
}

func TestSidecarPortCollisionRollsBack(t *testing.T) {
	var (
		listener net.Listener
		server   *arctic.Server
		sidecar  *arctic.WebSocketServer
		err      error
	)

	if listener, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	if server, err = arctic.NewServer(arctic.ServerConfig{BindAddress: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	if sidecar, err = arctic.NewWebSocketServer(arctic.WebSocketServerConfig{BindAddress: listener.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	if err = server.AddWebSocketSidecar(sidecar); err != nil {
		t.Fatal(err)
	}
	if err = server.Listen(); !errors.Is(err, arctic.ErrEndpointInUse) {
		t.Fatalf("expected ErrEndpointInUse, got %v", err)
	}
	if sidecar.Addr() != nil {
		t.Fatal("failed sidecar retained a listener")
	}
}

func TestGobWebSocketSidecar(t *testing.T) {
	var (
		server  *arctic.GobServer[webSocketGobMessage]
		sidecar *arctic.WebSocketServer
		client  *arctic.GobClient[webSocketGobMessage]
		replies chan string = make(chan string, 1)
		err     error
	)

	if err = arctic.RegisterGobType(webSocketGobMessage{}); err != nil {
		t.Fatal(err)
	}
	if server, err = arctic.NewGobServer[webSocketGobMessage](arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		BufferSize:  1024,
		Timeout:     time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	if sidecar, err = arctic.NewWebSocketServer(arctic.WebSocketServerConfig{BindAddress: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	if err = server.AddWebSocketSidecar(sidecar); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	server.OnClient(func(client *arctic.GobServerClient[webSocketGobMessage]) {
		client.OnMessage(func(message webSocketGobMessage) {
			_ = client.Send(webSocketGobMessage{Text: "reply:" + message.Text})
		})
	})
	go func() { _ = server.Listen() }()

	if client, err = arctic.NewGobClient[webSocketGobMessage](arctic.ClientConfig{
		ServerAddress: "ws://" + waitForWebSocketAddress(t, sidecar) + "/arctic",
		Protocol:      arctic.ProtocolWebSocket,
		BufferSize:    1024,
		Timeout:       time.Second,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	client.OnMessage(func(message webSocketGobMessage) { replies <- message.Text })
	if err = client.Connect(); err != nil {
		t.Fatal(err)
	}
	if err = client.Send(webSocketGobMessage{Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	assertString(t, replies, "reply:hello")
}

func waitForWebSocketAddress(t *testing.T, server *arctic.WebSocketServer) (address string) {
	var deadline time.Time = time.Now().Add(time.Second)

	t.Helper()
	for time.Now().Before(deadline) {
		if addr := server.Addr(); addr != nil {
			address = addr.String()
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("websocket sidecar did not start")
	return
}

func assertString(t *testing.T, values <-chan string, expected string) {
	t.Helper()
	select {
	case value := <-values:
		if value != expected {
			t.Fatalf("expected %q, got %q", expected, value)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %q", expected)
	}
}
