//go:build js

package arctic

import (
	"errors"
	"net"
)

// NewWebSocketServer reports that browser builds cannot host sidecars.
func NewWebSocketServer(config WebSocketServerConfig) (server *WebSocketServer, err error) {
	err = errors.New("arctic: websocket servers are not supported in browser builds")
	return
}

// AddWebSocketSidecar reports that browser builds cannot attach sidecars.
func (server *Server) AddWebSocketSidecar(sidecar *WebSocketServer) (err error) {
	err = errors.New("arctic: websocket servers are not supported in browser builds")
	return
}

// Addr returns no listener address because browser builds cannot host servers.
func (server *WebSocketServer) Addr() (addr net.Addr) {
	return
}

func (server *WebSocketServer) start(_ *Server, _ func(net.Conn)) (err error) {
	err = errors.New("arctic: websocket servers are not supported in browser builds")
	return
}

func (server *WebSocketServer) close() (err error) {
	return
}
