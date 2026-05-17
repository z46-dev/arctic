![Tests](https://img.shields.io/github/actions/workflow/status/z46-dev/arctic/tests.yml?branch=main&event=push&label=Tests&job=run-tests)
![Made with Golang](https://img.shields.io/badge/-Made_with_Golang-007d9c?logo=go&logoColor=white)

# arctic

A TCP/UDP client-server abstraction library written in Go

## Features

- TCP and UDP support
- Event-driven architecture
- Connection management
- Optional Gob encoding/decoding
- Graceful shutdown
- Configurable timeouts and buffer sizes
- Middleware support for custom processing
- Client metadata handshake support
- Optional unsafe zero-copy receive buffers

UDP is datagram-based. Arctic tracks server-side UDP peers as virtual clients keyed by remote address. Raw UDP remains best-effort and does not add reliability, ordering, retransmits, or remote connection teardown semantics.
Use TCP when you need reliable ordered delivery. Use UDP when you want fast best-effort datagrams and can tolerate packet loss or handle reliability at the application level.

## Client Metadata

Clients can attach JSON-compatible metadata to the connection. For TCP and Gob-over-TCP, Arctic sends it as an internal open handshake, so server `OnClient` handlers can read it before normal `OnMessage` traffic starts.

```go
client, err = arctic.NewClient(arctic.ClientConfig{
    ServerAddress: "localhost:8080",
    Metadata: map[string]any{
        "tenant": "acme",
        "trace_id": "demo-123",
        "debug": true,
    },
})
```

Read metadata from raw or Gob server clients with `Metadata()`:

```go
server.OnClient(func(client *arctic.ServerClient) {
    var metadata map[string]any = client.Metadata()
    log.Printf("client metadata: %#v", metadata)
})
```

> Metadata values should be JSON-compatible: strings, booleans, numbers, `nil`, arrays, and objects. Numeric values are decoded as `json.Number` on the server side.

> UDP metadata is best-effort. Arctic sends it as an initial datagram, but UDP can drop or reorder datagrams, so a UDP `OnClient` handler may run before metadata is available. If a valid metadata datagram arrives later, Arctic stores it for future `Metadata()` calls, but `OnClient` is not called again.

## Unsafe Zero-Copy

By default, Arctic gives each `OnMessage` call a safe message slice that can be retained after the handler returns. Set `UnsafeZeroCopy: true` on `ClientConfig` or `ServerConfig` to reduce receive-side allocations by reusing internal read buffers.

With `UnsafeZeroCopy` enabled, the `[]byte` passed to `OnMessage` is only valid during that handler call. Copy it before storing it, sending it to another goroutine, or keeping it after the handler returns:

```go
client.OnMessage(func(message []byte) {
    saved := append([]byte{}, message...)
    _ = saved
})
```

This option can improve allocation and GC behavior for raw TCP and UDP messages. It does not remove the operating system socket copy, and it does not affect TCP Gob stream decoding.

## Testing

Tests are organized by purpose:

- `tests/implementation`: TCP, UDP, Gob, close handling, and zero-copy behavior tests
- `tests/coverage`: constructor, validation, registry, and coverage-focused smoke tests
- `tests/benchmark`: TCP and UDP benchmark suites
- `tests/internal/testutil`: shared test helpers

Useful commands:

```bash
go test ./...
go test ./tests/coverage ./tests/implementation -coverpkg=github.com/z46-dev/arctic -coverprofile=coverage.out
go tool cover -func=coverage.out
go test ./tests/benchmark -bench . -benchmem
```

## Basic Usage

Set up a server:

```go
package main

import (
    "log"
    "fmt"
    "time"

    "github.com/z46-dev/arctic"
)

func main() {
    var (
        server *arctic.Server
        err error
    )

    if server, err = arctic.NewServer(arctic.ServerConfig{
        BindAddress: "[::]:8080",
        BufferSize: 1024,
        Timeout: 5 * time.Second,
    }); err != nil {
        log.Fatalf("Failed to create server: %v", err)
    }

    server.OnClient(func(client *arctic.ServerClient) {
        var logPrefix string = fmt.Sprintf("[%d | %s]", client.ID(), client.RemoteAddr())

        log.Printf("%s Client connected", logPrefix)

        client.OnMessage(func(msg []byte) {
            log.Printf("%s Received message: %s", logPrefix, string(msg))
            client.Send([]byte("Message received"))
        })

        client.OnClose(func() {
            log.Printf("%s Client disconnected", logPrefix)
        })

        client.OnError(func(err error) {
            log.Printf("%s Client error: %v", logPrefix, err)
        })
    })

    if err = server.Listen(); err != nil {
        log.Fatalf("Failed to start server: %v", err)
    }
}
```

Set up a client:

```go
package main

import (
    "log"
    "time"

    "github.com/z46-dev/arctic"
)

func main() {
    var (
        client *arctic.Client
        err error
    )

    if client, err = arctic.NewClient(arctic.ClientConfig{
        ServerAddress: "localhost:8080",
        BufferSize: 1024,
        Timeout: 5 * time.Second,
    }); err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }

    client.OnMessage(func(msg []byte) {
        log.Printf("Received message from server: %s", string(msg))
    })

    client.OnClose(func() {
        log.Println("Connection closed by server")
    })

    client.OnError(func(err error) {
        log.Printf("Client error: %v", err)
    })

    if err = client.Connect(); err != nil {
        log.Fatalf("Failed to connect to server: %v", err)
    }

    // Send a message to the server
    if err = client.Send([]byte("Hello, Server!")); err != nil {
        log.Printf("Failed to send message: %v", err)
    }

    // Keep the client running to receive messages
    time.Sleep(time.Second)
}
```

See runnable examples in the `examples` directory for more usage patterns, including UDP, Gob encoding, and metadata.

Or run them yourself with:

```bash
go run github.com/z46-dev/arctic/examples/tcp_basic@latest
go run github.com/z46-dev/arctic/examples/udp_basic@latest
go run github.com/z46-dev/arctic/examples/gob_basic@latest
go run github.com/z46-dev/arctic/examples/metadata_basic@latest
```

> You can pass `-addr <address>` to override the default bind/connect address for the example. For example:

```bash
go run github.com/z46-dev/arctic/examples/tcp_basic@latest -addr localhost:9090
```
