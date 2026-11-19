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

// NewClient creates a raw client for the configured transport.
func NewClient(config ClientConfig) (client *Client, err error) {
	config = normalizeClientConfig(config)

	if err = validateProtocol(config.Protocol); err != nil {
		return
	}

	client = newClient(config, nil)
	return
}

// NewServer creates a TCP or UDP server.
func NewServer(config ServerConfig) (server *Server, err error) {
	config = normalizeServerConfig(config)

	if err = validateServerProtocol(config.Protocol); err != nil {
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

// NewContractClient creates a typed contract client over the raw client transport.
func NewContractClient[ContractType any](config ClientConfig, factory ContractFactory[ContractType]) (client *ContractClient[ContractType], err error) {
	if factory == nil {
		err = ErrContractFactoryMissing
		return
	}

	var raw *Client
	if raw, err = NewClient(config); err != nil {
		return
	}

	client = &ContractClient[ContractType]{Client: raw, Contract: factory(NewContractPeer(raw))}
	return
}

// NewContractServer creates a typed contract server over the raw server transport.
func NewContractServer[ContractType any](config ServerConfig, factory ContractFactory[ContractType]) (server *ContractServer[ContractType], err error) {
	if factory == nil {
		err = ErrContractFactoryMissing
		return
	}

	var raw *Server
	if raw, err = NewServer(config); err != nil {
		return
	}

	server = &ContractServer[ContractType]{Server: raw, factory: factory}
	return
}

func validateServerProtocol(protocol Protocol) (err error) {
	if protocol != ProtocolTCP && protocol != ProtocolUDP {
		err = fmt.Errorf("%w: %s", ErrProtocolUnsupported, protocol)
	}
	return
}

// NewGobClient creates a typed Gob client for the configured transport.
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

// NewGobServer creates a typed Gob server.
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
	if protocol != ProtocolTCP && protocol != ProtocolUDP && protocol != ProtocolWebSocket {
		err = fmt.Errorf("%w: %s", ErrProtocolUnsupported, protocol)
		return
	}

	return
}

func newClient(config ClientConfig, conn net.Conn) (client *Client) {
	var metadata map[string]any = cloneMetadata(config.Metadata)
	config.Metadata = metadata

	client = &Client{
		conn:     conn,
		config:   config,
		metadata: metadata,
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
