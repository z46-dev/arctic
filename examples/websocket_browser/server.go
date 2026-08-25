//go:build !js

package main

import (
	_ "embed"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/z46-dev/arctic"
)

//go:embed index.html
var indexHTML []byte

func main() {
	var (
		server       *arctic.Server
		sidecar      *arctic.WebSocketServer
		wasmExecPath string = filepath.Join(runtime.GOROOT(), "lib", "wasm", "wasm_exec.js")
		wasmExec     []byte
		err          error
	)

	if server, err = arctic.NewServer(arctic.ServerConfig{
		BindAddress: "127.0.0.1:8082",
		Protocol:    arctic.ProtocolTCP,
		BufferSize:  1024,
		Timeout:     5 * time.Second,
	}); err != nil {
		log.Fatal(err)
	}

	if sidecar, err = arctic.NewWebSocketServer(arctic.WebSocketServerConfig{
		BindAddress: "127.0.0.1:8081",
		Path:        "/arctic",
		OriginPatterns: []string{
			"localhost:8080",
			"127.0.0.1:8080",
		},
		Timeout: 5 * time.Second,
	}); err != nil {
		log.Fatal(err)
	}

	if err = server.AddWebSocketSidecar(sidecar); err != nil {
		log.Fatal(err)
	}

	server.OnClient(func(client *arctic.ServerClient) {
		var sendErr error

		log.Printf("browser client %d connected with metadata %#v", client.ID(), client.Metadata())
		client.OnMessage(func(message []byte) {
			log.Printf("browser sent: %s", message)
			if sendErr = client.Send([]byte("Hello from the native Arctic server")); sendErr != nil {
				log.Printf("server send failed: %v", sendErr)
			}
		})
	})

	go func() {
		var listenErr error

		if listenErr = server.Listen(); listenErr != nil {
			log.Fatal(listenErr)
		}
	}()

	defer server.Close()

	if wasmExec, err = os.ReadFile(wasmExecPath); err != nil {
		log.Fatalf("read %s: %v", wasmExecPath, err)
	}

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			http.NotFound(writer, request)
			return
		}

		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = writer.Write(indexHTML)
	})

	http.HandleFunc("/wasm_exec.js", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/javascript")
		_, _ = writer.Write(wasmExec)
	})

	http.Handle("/client.wasm", http.FileServer(http.Dir("examples/websocket_browser")))

	log.Println("open http://localhost:8080 in your browser")
	log.Fatal(http.ListenAndServe("127.0.0.1:8080", nil))
}
