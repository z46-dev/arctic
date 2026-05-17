package util

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/z46-dev/arctic"
)

type (
	AddressProvider interface {
		Addr() net.Addr
	}

	GobMessage struct {
		Text  string
		Count int
	}
)

func StartServer(t testing.TB, server *arctic.Server) (listenDone chan error) {
	t.Helper()

	listenDone = make(chan error, 1)

	go func() {
		listenDone <- server.Listen()
	}()

	return
}

func StartGobServer[MessageType any](
	t testing.TB,
	server *arctic.GobServer[MessageType],
) (listenDone chan error) {
	t.Helper()

	listenDone = make(chan error, 1)

	go func() {
		listenDone <- server.Listen()
	}()

	return
}

func WaitForAddress(t testing.TB, provider AddressProvider) (address string) {
	t.Helper()

	var timeout <-chan time.Time = time.After(time.Second)
	var ticker *time.Ticker = time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatal("timed out waiting for server address")
		case <-ticker.C:
			var addr net.Addr = provider.Addr()

			if addr != nil {
				address = addr.String()
				return
			}
		}
	}
}

func CloseServer(t testing.TB, server *arctic.Server, listenDone <-chan error) {
	t.Helper()

	var err error

	if err = server.Close(); err != nil {
		t.Fatalf("close server: %v", err)
	}

	WaitForListenExit(t, listenDone)
}

func CloseGobServer[MessageType any](
	t testing.TB,
	server *arctic.GobServer[MessageType],
	listenDone <-chan error,
) {
	t.Helper()

	var err error

	if err = server.Close(); err != nil {
		t.Fatalf("close gob server: %v", err)
	}

	WaitForListenExit(t, listenDone)
}

func WaitForListenExit(t testing.TB, listenDone <-chan error) {
	t.Helper()

	var err error

	select {
	case err = <-listenDone:
		if err != nil {
			t.Fatalf("listen returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for listener to close")
	}
}

func AssertNoAsyncError(t testing.TB, asyncErrors <-chan error) {
	t.Helper()

	for {
		var err error

		select {
		case err = <-asyncErrors:
			if IsExpectedAsyncCloseError(err) {
				continue
			}

			t.Fatalf("unexpected async error: %v", err)
		default:
			return
		}
	}
}

func NewUDPClient(
	t testing.TB,
	address string,
	asyncErrors chan<- error,
) (client *arctic.Client, messages chan []byte) {
	t.Helper()

	var err error

	if client, err = arctic.NewClient(arctic.ClientConfig{
		ServerAddress: address,
		Protocol:      arctic.ProtocolUDP,
		BufferSize:    1024,
		Timeout:       time.Second,
	}); err != nil {
		t.Fatalf("new udp client: %v", err)
	}

	messages = make(chan []byte, 1)

	client.OnError(func(err error) {
		asyncErrors <- err
	})

	client.OnMessage(func(message []byte) {
		messages <- append([]byte{}, message...)
	})

	if err = client.Connect(); err != nil {
		t.Fatalf("connect udp client: %v", err)
	}

	return
}

func CloseClient(t testing.TB, client *arctic.Client) {
	t.Helper()

	var err error

	if client == nil {
		return
	}

	if err = client.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
}

func CloseClients(t testing.TB, clients []*arctic.Client) {
	t.Helper()

	for _, client := range clients {
		CloseClient(t, client)
	}
}

func AssertMessage(t testing.TB, messages <-chan []byte, expected string) {
	t.Helper()

	select {
	case message := <-messages:
		if string(message) != expected {
			t.Fatalf("expected %q, got %q", expected, string(message))
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %q", expected)
	}
}

func AssertClosed(t testing.TB, closed <-chan struct{}, name string) {
	t.Helper()

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func WaitBenchmarkResponse(t testing.TB, responses <-chan []byte) {
	t.Helper()

	select {
	case <-responses:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for benchmark response")
	}
}

func IsExpectedAsyncCloseError(err error) (expected bool) {
	if err == nil {
		return
	}

	if errors.Is(err, net.ErrClosed) {
		expected = true
		return
	}

	if strings.Contains(err.Error(), "use of closed network connection") {
		expected = true
	}

	return
}
