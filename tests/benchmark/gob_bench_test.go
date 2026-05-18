package benchmark

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/z46-dev/arctic"
	"github.com/z46-dev/arctic/tests/util"
)

func BenchmarkGobTCPRoundTrip(b *testing.B) {
	benchmarkGobTCPClients(b, 1)
}

func BenchmarkGobTCPScalability(b *testing.B) {
	var clientCounts []int = []int{1, 8, 32, 128}

	for _, clientCount := range clientCounts {
		var name string = fmt.Sprintf("clients=%d", clientCount)

		b.Run(name, func(b *testing.B) {
			benchmarkGobTCPClients(b, clientCount)
		})
	}
}

func BenchmarkGobUDPRoundTrip(b *testing.B) {
	benchmarkGobUDPClients(b, 1)
}

func BenchmarkGobUDPScalability(b *testing.B) {
	var clientCounts []int = []int{1, 8, 32, 128}

	for _, clientCount := range clientCounts {
		var name string = fmt.Sprintf("clients=%d", clientCount)

		b.Run(name, func(b *testing.B) {
			benchmarkGobUDPClients(b, clientCount)
		})
	}
}

func benchmarkGobTCPClients(b *testing.B, clientCount int) {
	var serverConfig arctic.ServerConfig = arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
	}
	var clientConfig func(string) arctic.ClientConfig = func(address string) (config arctic.ClientConfig) {
		config = arctic.ClientConfig{
			ServerAddress: address,
		}

		return
	}

	benchmarkGobClients(b, clientCount, serverConfig, clientConfig)
}

func benchmarkGobUDPClients(b *testing.B, clientCount int) {
	var serverConfig arctic.ServerConfig = arctic.ServerConfig{
		BindAddress: "127.0.0.1:0",
		Protocol:    arctic.ProtocolUDP,
		BufferSize:  1024,
	}
	var clientConfig func(string) arctic.ClientConfig = func(address string) (config arctic.ClientConfig) {
		config = arctic.ClientConfig{
			ServerAddress: address,
			Protocol:      arctic.ProtocolUDP,
			BufferSize:    1024,
		}

		return
	}

	benchmarkGobClients(b, clientCount, serverConfig, clientConfig)
}

func benchmarkGobClients(
	b *testing.B,
	clientCount int,
	serverConfig arctic.ServerConfig,
	clientConfig func(string) arctic.ClientConfig,
) {
	var (
		server      *arctic.GobServer[util.GobMessage]
		err         error
		address     string
		listenDone  chan error
		asyncErrors chan error = make(chan error, clientCount+8)
		clients     []*arctic.GobClient[util.GobMessage]
		responses   []chan util.GobMessage
		payload     util.GobMessage = util.GobMessage{Text: "ping", Count: 41}
		started     time.Time
		elapsed     time.Duration
	)

	b.Helper()
	b.ReportAllocs()

	if err = arctic.RegisterGobType(util.GobMessage{}); err != nil {
		b.Fatalf("register gob type: %v", err)
	}

	if server, err = arctic.NewGobServer[util.GobMessage](serverConfig); err != nil {
		b.Fatalf("new gob server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.GobServerClient[util.GobMessage]) {
		client.OnMessage(func(message util.GobMessage) {
			var err error

			if err = client.Send(message); err != nil {
				asyncErrors <- err
			}
		})
	})

	listenDone = util.StartGobServer(b, server)
	address = util.WaitForAddress(b, server)
	clients, responses = connectBenchmarkGobClients(b, address, clientCount, clientConfig, asyncErrors)

	b.Cleanup(func() {
		closeGobBenchmarkClients(b, clients)
		util.CloseGobServer(b, server, listenDone)
		util.AssertNoAsyncError(b, asyncErrors)
	})

	warmBenchmarkGobClients(b, clients, responses, payload)

	b.ResetTimer()
	started = time.Now()

	if clientCount == 1 {
		benchmarkSingleGobClient(b, clients[0], responses[0], payload)
	} else {
		benchmarkParallelGobClients(b, clients, responses, payload, asyncErrors)
	}

	elapsed = time.Since(started)
	b.StopTimer()

	b.ReportMetric(float64(clientCount), "clients")

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
}

func connectBenchmarkGobClients(
	b *testing.B,
	address string,
	clientCount int,
	clientConfig func(string) arctic.ClientConfig,
	asyncErrors chan<- error,
) (clients []*arctic.GobClient[util.GobMessage], responses []chan util.GobMessage) {
	clients = make([]*arctic.GobClient[util.GobMessage], clientCount)
	responses = make([]chan util.GobMessage, clientCount)

	for index := range clients {
		var client *arctic.GobClient[util.GobMessage]
		var err error
		var received chan util.GobMessage = make(chan util.GobMessage, 256)

		if client, err = arctic.NewGobClient[util.GobMessage](clientConfig(address)); err != nil {
			b.Fatalf("new gob client: %v", err)
		}

		client.OnError(func(err error) {
			asyncErrors <- err
		})

		client.OnMessage(func(message util.GobMessage) {
			received <- message
		})

		if err = client.Connect(); err != nil {
			b.Fatalf("connect gob client: %v", err)
		}

		clients[index] = client
		responses[index] = received
	}

	return
}

func closeGobBenchmarkClients(b *testing.B, clients []*arctic.GobClient[util.GobMessage]) {
	b.Helper()

	for _, client := range clients {
		var err error

		if client == nil {
			continue
		}

		if err = client.Close(); err != nil {
			b.Fatalf("close gob client: %v", err)
		}
	}
}

func warmBenchmarkGobClients(
	b *testing.B,
	clients []*arctic.GobClient[util.GobMessage],
	responses []chan util.GobMessage,
	payload util.GobMessage,
) {
	b.Helper()

	for index, client := range clients {
		var err error

		if err = client.Send(payload); err != nil {
			b.Fatalf("warm gob send: %v", err)
		}

		waitBenchmarkGobResponse(b, responses[index])
	}
}

func waitBenchmarkGobResponse(b *testing.B, responses <-chan util.GobMessage) {
	b.Helper()

	select {
	case <-responses:
	case <-time.After(time.Second):
		b.Fatal("timed out waiting for gob benchmark response")
	}
}

func benchmarkSingleGobClient(
	b *testing.B,
	client *arctic.GobClient[util.GobMessage],
	responses <-chan util.GobMessage,
	payload util.GobMessage,
) {
	var err error

	for index := 0; index < b.N; index++ {
		if err = client.Send(payload); err != nil {
			b.Fatalf("send gob: %v", err)
		}

		<-responses
	}
}

func benchmarkParallelGobClients(
	b *testing.B,
	clients []*arctic.GobClient[util.GobMessage],
	responses []chan util.GobMessage,
	payload util.GobMessage,
	asyncErrors chan error,
) {
	var (
		nextOp int64
		failed atomic.Bool
		group  sync.WaitGroup
	)

	group.Add(len(clients))

	for index, client := range clients {
		go func(index int, client *arctic.GobClient[util.GobMessage]) {
			defer group.Done()

			for {
				var op int64
				var err error

				if failed.Load() {
					return
				}

				op = atomic.AddInt64(&nextOp, 1)

				if int(op) > b.N {
					return
				}

				if err = client.Send(payload); err != nil {
					if failed.CompareAndSwap(false, true) {
						asyncErrors <- err
					}

					return
				}

				<-responses[index]
			}
		}(index, client)
	}

	group.Wait()

	if failed.Load() {
		util.AssertNoAsyncError(b, asyncErrors)
	}
}
