package arctic

import (
	"fmt"
	"net"
)

const (
	defaultBufferSize      int = 4096
	frameHeaderSize        int = 4
	defaultUDPSocketShards int = 8
)

func NewClient(config ClientConfig) (client *Client, err error) {
	config = normalizeClientConfig(config)

	if err = validateProtocol(config.Protocol); err != nil {
		return
	}

	client = newClient(config, nil)
	return
}

func NewServer(config ServerConfig) (server *Server, err error) {
	config = normalizeServerConfig(config)

	if err = validateProtocol(config.Protocol); err != nil {
		return
	}

	server = &Server{
		config:     config,
		clients:    map[int]*ServerClient{},
		udpClients: map[udpClientKey]*ServerClient{},
		done:       make(chan struct{}),
	}

	return
}

func NewGobClient[MessageType any](config ClientConfig) (client *GobClient[MessageType], err error) {
	var raw *Client

	if err = requireGobTypeRegistered[MessageType](); err != nil {
		return
	}

	if raw, err = NewClient(config); err != nil {
		return
	}

	client = &GobClient[MessageType]{
		Client: raw,
	}

	return
}

func NewGobServer[MessageType any](config ServerConfig) (server *GobServer[MessageType], err error) {
	var raw *Server

	if err = requireGobTypeRegistered[MessageType](); err != nil {
		return
	}

	if raw, err = NewServer(config); err != nil {
		return
	}

	server = &GobServer[MessageType]{
		Server:     raw,
		gobClients: map[int]*GobServerClient[MessageType]{},
	}

	return
}

func normalizeClientConfig(config ClientConfig) (normalized ClientConfig) {
	normalized = config

	if normalized.Protocol == "" {
		normalized.Protocol = ProtocolTCP
	}

	if normalized.BufferSize <= 0 {
		normalized.BufferSize = defaultBufferSize
	}

	return
}

func normalizeServerConfig(config ServerConfig) (normalized ServerConfig) {
	normalized = config

	if normalized.Protocol == "" {
		normalized.Protocol = ProtocolTCP
	}

	if normalized.BufferSize <= 0 {
		normalized.BufferSize = defaultBufferSize
	}

	return
}

func validateProtocol(protocol Protocol) (err error) {
	if protocol != ProtocolTCP && protocol != ProtocolUDP {
		err = fmt.Errorf("%w: %s", ErrProtocolUnsupported, protocol)
		return
	}

	return
}

func newClient(config ClientConfig, conn net.Conn) (client *Client) {
	client = &Client{
		conn:     conn,
		config:   config,
		ownsConn: true,
		done:     make(chan struct{}),
	}

	return
}

func clientConfigFromServer(config ServerConfig) (clientConfig ClientConfig) {
	clientConfig = ClientConfig{
		Protocol:       config.Protocol,
		BufferSize:     config.BufferSize,
		Timeout:        config.Timeout,
		UnsafeZeroCopy: config.UnsafeZeroCopy,
	}

	return
}
