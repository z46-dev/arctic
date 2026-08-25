//go:build !js

package arctic

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/coder/websocket"
)

// NewWebSocketServer creates a sidecar that can be attached to an Arctic server.
func NewWebSocketServer(config WebSocketServerConfig) (server *WebSocketServer, err error) {
	if config.BindAddress == "" {
		return nil, fmt.Errorf("arctic: websocket bind address is required")
	}
	if config.Path == "" {
		config.Path = "/arctic"
	}
	if !strings.HasPrefix(config.Path, "/") {
		return nil, fmt.Errorf("arctic: websocket path must start with /")
	}
	config.OriginPatterns = append([]string{}, config.OriginPatterns...)
	server = &WebSocketServer{config: config}
	return
}

// AddWebSocketSidecar attaches a WebSocket listener to the server's lifecycle.
func (server *Server) AddWebSocketSidecar(sidecar *WebSocketServer) (err error) {
	if sidecar == nil {
		return errors.New("arctic: websocket sidecar is nil")
	}

	server.mutex.Lock()
	defer server.mutex.Unlock()
	if server.listening {
		return ErrServerAlreadyListening
	}
	for _, current := range server.webSocketSidecars {
		if current == sidecar {
			return ErrSidecarAlreadyAttached
		}
	}

	sidecar.mutex.Lock()
	if sidecar.parent != nil && sidecar.parent != server {
		sidecar.mutex.Unlock()
		return ErrSidecarAlreadyAttached
	}
	sidecar.parent = server
	sidecar.mutex.Unlock()
	server.webSocketSidecars = append(server.webSocketSidecars, sidecar)
	return
}

// Addr returns the bound address after the sidecar starts listening.
func (server *WebSocketServer) Addr() (addr net.Addr) {
	server.mutex.RLock()
	if server.listener != nil {
		addr = server.listener.Addr()
	}
	server.mutex.RUnlock()
	return
}

func (server *WebSocketServer) start(parent *Server, accept func(net.Conn)) (err error) {
	var (
		listener net.Listener
		release  func()
		mux      *http.ServeMux
		web      *http.Server
	)

	if release, err = reserveEndpoint("tcp", server.config.BindAddress); err != nil {
		return
	}

	if listener, err = net.Listen("tcp", server.config.BindAddress); err != nil {
		release()
		err = endpointListenError("tcp", server.config.BindAddress, err)
		return
	}

	mux = http.NewServeMux()
	mux.HandleFunc(server.config.Path, func(writer http.ResponseWriter, request *http.Request) {
		var (
			webSocket *websocket.Conn
			acceptErr error
		)

		if webSocket, acceptErr = websocket.Accept(writer, request, &websocket.AcceptOptions{
			Subprotocols:    []string{arcticWebSocketSubprotocol},
			OriginPatterns:  append([]string{}, server.config.OriginPatterns...),
			CompressionMode: websocket.CompressionDisabled,
		}); acceptErr != nil {
			parent.handleError(acceptErr)
			return
		}

		accept(websocket.NetConn(context.Background(), webSocket, websocket.MessageBinary))
	})

	web = &http.Server{Handler: mux, ReadHeaderTimeout: server.config.Timeout}
	server.mutex.Lock()
	if server.listener != nil {
		server.mutex.Unlock()
		_ = listener.Close()
		release()
		return ErrServerAlreadyListening
	}
	server.listener = listener
	server.server = web
	server.release = release
	server.mutex.Unlock()

	go func() {
		var serveErr error

		if serveErr = web.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) && !errors.Is(serveErr, net.ErrClosed) {
			parent.setListenerError(serveErr)
			parent.handleError(serveErr)
			_ = parent.Close()
		}
	}()
	return
}

func (server *WebSocketServer) close() (err error) {
	var (
		httpServer *http.Server
		listener   net.Listener
		release    func()
	)

	server.mutex.Lock()
	httpServer, _ = server.server.(*http.Server)
	listener = server.listener
	release = server.release
	server.server = nil
	server.listener = nil
	server.release = nil
	server.mutex.Unlock()

	if httpServer != nil {
		err = httpServer.Close()
	} else if listener != nil {
		err = listener.Close()
	}
	if release != nil {
		release()
	}

	return
}
