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

func BenchmarkUDPRoundTrip(b *testing.B) {
	var payload []byte = []byte("ping")

	for _, mode := range zeroCopyBenchModes() {
		b.Run(mode.name, func(b *testing.B) {
			benchmarkUDPClients(b, 1, payload, mode.enabled)
		})
	}
}

func BenchmarkUDPScalability(b *testing.B) {
	var clientCounts []int = []int{1, 8, 32, 128}
	var payload []byte = []byte("ping")

	for _, mode := range zeroCopyBenchModes() {
		b.Run(mode.name, func(b *testing.B) {
			for _, clientCount := range clientCounts {
				var name string = fmt.Sprintf("clients=%d", clientCount)

				b.Run(name, func(b *testing.B) {
					benchmarkUDPClients(b, clientCount, payload, mode.enabled)
				})
			}
		})
	}
}

func benchmarkUDPClients(b *testing.B, clientCount int, payload []byte, zeroCopy bool) {
	var (
		server      *arctic.Server
		err         error
		address     string
		listenDone  chan error
		asyncErrors chan error = make(chan error, clientCount+8)
		clients     []*arctic.Client
		responses   []chan []byte
		started     time.Time
		elapsed     time.Duration
	)

	b.Helper()
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress:    "127.0.0.1:0",
		Protocol:       arctic.ProtocolUDP,
		BufferSize:     1024,
		UnsafeZeroCopy: zeroCopy,
	}); err != nil {
		b.Fatalf("new udp server: %v", err)
	}

	server.OnError(func(err error) {
		asyncErrors <- err
	})

	server.OnClient(func(client *arctic.ServerClient) {
		client.OnMessage(func(message []byte) {
			var err error

			if err = client.Send(message); err != nil {
				asyncErrors <- err
			}
		})
	})

	listenDone = util.StartServer(b, server)
	address = util.WaitForAddress(b, server)
	clients, responses = connectBenchmarkUDPClients(b, address, clientCount, asyncErrors, zeroCopy)

	b.Cleanup(func() {
		util.CloseClients(b, clients)
		util.CloseServer(b, server, listenDone)
		util.AssertNoAsyncError(b, asyncErrors)
	})

	warmBenchmarkClients(b, clients, responses, payload)

	b.ResetTimer()
	started = time.Now()

	if clientCount == 1 {
		benchmarkSingleTCPClient(b, clients[0], responses[0], payload)
	} else {
		benchmarkParallelUDPClients(b, clients, responses, payload, asyncErrors)
	}

	elapsed = time.Since(started)
	b.StopTimer()

	b.ReportMetric(float64(clientCount), "clients")
	b.ReportMetric(zeroCopyMetric(zeroCopy), "zero_copy")

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	}
}

func connectBenchmarkUDPClients(
	b *testing.B,
	address string,
	clientCount int,
	asyncErrors chan<- error,
	zeroCopy bool,
) (clients []*arctic.Client, responses []chan []byte) {
	clients = make([]*arctic.Client, clientCount)
	responses = make([]chan []byte, clientCount)

	for index := range clients {
		var client *arctic.Client
		var err error
		var received chan []byte = make(chan []byte, 256)

		if client, err = arctic.NewClient(arctic.ClientConfig{
			ServerAddress:  address,
			Protocol:       arctic.ProtocolUDP,
			BufferSize:     1024,
			UnsafeZeroCopy: zeroCopy,
		}); err != nil {
			b.Fatalf("new udp client: %v", err)
		}

		client.OnError(func(err error) {
			asyncErrors <- err
		})

		client.OnMessage(func(message []byte) {
			received <- message
		})

		if err = client.Connect(); err != nil {
			b.Fatalf("connect udp client: %v", err)
		}

		clients[index] = client
		responses[index] = received
	}

	return
}

func benchmarkParallelUDPClients(
	b *testing.B,
	clients []*arctic.Client,
	responses []chan []byte,
	payload []byte,
	asyncErrors chan error,
) {
	var (
		nextOp int64
		failed atomic.Bool
		group  sync.WaitGroup
	)

	group.Add(len(clients))

	for index, client := range clients {
		go func(index int, client *arctic.Client) {
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
