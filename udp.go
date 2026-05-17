package arctic

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync"
	"time"
)

type (
	udpClientHandler  func(*ServerClient)
	udpMessageHandler func(*ServerClient, []byte)
)

func (client *Client) connectUDP() (err error) {
	var (
		remoteAddr *net.UDPAddr
		conn       *net.UDPConn
	)

	if remoteAddr, err = net.ResolveUDPAddr(string(ProtocolUDP), client.config.ServerAddress); err != nil {
		return
	}

	if conn, err = net.DialUDP(string(ProtocolUDP), nil, remoteAddr); err != nil {
		return
	}

	client.setUDPConnection(conn, nil, true, nil, nil)

	if err = client.sendMetadataHandshake(); err != nil {
		var _ error = conn.Close()
		client.setUDPConnection(nil, nil, true, nil, nil)
		return
	}

	go client.readUDPMessages()
	return
}

func (client *Client) sendUDP(message []byte) (err error) {
	if err = validateFrameSize(message, client.config.BufferSize); err != nil {
		return
	}

	err = client.writeUDPDatagram(message)
	return
}

func (client *Client) writeUDPDatagram(message []byte) (err error) {
	var (
		udpConn     *net.UDPConn
		udpAddrPort udpClientKey
		writer      *sync.Mutex
		hasDeadline bool = client.config.Timeout > 0
		written     int
	)

	udpConn, udpAddrPort, writer = client.udpSendTarget()

	if udpConn == nil {
		err = ErrClientNotConnected
		return
	}

	if !hasDeadline {
		written, err = writeUDPDatagram(udpConn, udpAddrPort, message)

		if err != nil {
			client.handleError(err)
			return
		}

		if written != len(message) {
			err = io.ErrShortWrite
			client.handleError(err)
			return
		}

		return
	}

	writer.Lock()
	defer writer.Unlock()

	if err = udpConn.SetWriteDeadline(time.Now().Add(client.config.Timeout)); err != nil {
		return
	}

	written, err = writeUDPDatagram(udpConn, udpAddrPort, message)
	var _ error = udpConn.SetWriteDeadline(time.Time{})

	if err != nil {
		client.handleError(err)
		return
	}

	if written != len(message) {
		err = io.ErrShortWrite
		client.handleError(err)
		return
	}

	return
}

func writeUDPDatagram(udpConn *net.UDPConn, udpAddrPort udpClientKey, message []byte) (written int, err error) {
	if !udpAddrPort.IsValid() {
		written, err = udpConn.Write(message)
		return
	}

	written, err = udpConn.WriteToUDPAddrPort(message, udpAddrPort)
	return
}

func (client *Client) readUDPMessages() {
	var conn *net.UDPConn = client.udpConnection()
	var buffer []byte = make([]byte, udpReadBufferSize(client.config.BufferSize))

	defer client.Close()

	for conn != nil {
		var (
			count int
			err   error
		)

		if count, err = conn.Read(buffer); err != nil {
			client.handleError(err)
			return
		}

		if count > client.config.BufferSize {
			client.handleError(errMessageTooLarge(count, client.config.BufferSize))
			continue
		}

		client.dispatchMessage(client.receivedDatagram(buffer[:count]))
	}
}

func (server *Server) listenUDP(onClient udpClientHandler, onMessage udpMessageHandler) (err error) {
	var conns []*net.UDPConn

	if conns, err = listenUDPConns(server.config); err != nil {
		return
	}

	if err = server.setUDPConns(conns); err != nil {
		var closeErr error = closeUDPConns(conns)

		if closeErr != nil {
			err = fmt.Errorf("%w: %v", err, closeErr)
		}

		return
	}

	err = server.readUDP(conns, onClient, onMessage)
	return
}

func listenUDPConns(config ServerConfig) (conns []*net.UDPConn, err error) {
	var (
		conn        *net.UDPConn
		address     string = config.BindAddress
		socketCount int    = udpSocketShardCount(config)
	)

	if conn, err = listenUDPConn(address, socketCount > 1); err != nil {
		if socketCount <= 1 {
			return
		}

		socketCount = 1

		if conn, err = listenUDPConn(address, false); err != nil {
			return
		}
	}

	conns = append(conns, conn)

	if socketCount <= 1 {
		return
	}

	address = conn.LocalAddr().String()

	for len(conns) < socketCount {
		if conn, err = listenUDPConn(address, true); err != nil {
			err = nil
			return
		}

		conns = append(conns, conn)
	}

	return
}

func listenUDPConn(address string, reusePort bool) (conn *net.UDPConn, err error) {
	var (
		listenConfig net.ListenConfig
		packetConn   net.PacketConn
		ok           bool
	)

	if reusePort {
		listenConfig.Control = setUDPReusePort
	}

	if packetConn, err = listenConfig.ListenPacket(context.Background(), string(ProtocolUDP), address); err != nil {
		return
	}

	if conn, ok = packetConn.(*net.UDPConn); !ok {
		err = fmt.Errorf("%w: udp listen did not return udp conn", ErrProtocolUnsupported)
		var _ error = packetConn.Close()
		return
	}

	return
}

func closeUDPConns(conns []*net.UDPConn) (err error) {
	for _, conn := range conns {
		var closeErr error

		if conn == nil {
			continue
		}

		if closeErr = conn.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}

	return
}

func (server *Server) readUDP(conns []*net.UDPConn, onClient udpClientHandler, onMessage udpMessageHandler) (err error) {
	if len(conns) == 1 {
		err = server.readUDPLoop(conns[0], onClient, onMessage)
		return
	}

	err = server.readUDPShards(conns, onClient, onMessage)
	return
}

func (server *Server) readUDPLoop(
	conn *net.UDPConn,
	onClient udpClientHandler,
	onMessage udpMessageHandler,
) (err error) {
	var buffer []byte = make([]byte, udpReadBufferSize(metadataBufferSize(server.config.BufferSize)))

	for {
		var (
			count      int
			addr       udpClientKey
			client     *ServerClient
			created    bool
			metadata   map[string]any
			isMetadata bool
		)

		if count, addr, err = conn.ReadFromUDPAddrPort(buffer); err != nil {
			if isExpectedCloseError(err) {
				err = nil
			} else {
				server.handleError(err)
			}

			return
		}

		client = server.udpClientSnapshot(addr)

		if client == nil {
			if metadata, isMetadata, err = decodeMetadataHandshake(buffer[:count]); err != nil {
				server.handleError(err)
				continue
			}

			if isMetadata {
				client, created = server.udpClientFor(conn, addr)
				client.setMetadata(metadata)

				if created && onClient != nil {
					onClient(client)
				}

				continue
			}
		}

		if count > server.config.BufferSize {
			server.handleError(errMessageTooLarge(count, server.config.BufferSize))
			continue
		}

		client, created = server.udpClientFor(conn, addr)

		if created && onClient != nil {
			onClient(client)
		}

		if onMessage != nil {
			onMessage(client, client.receivedDatagram(buffer[:count]))
		}
	}
}

func (server *Server) readUDPShards(conns []*net.UDPConn, onClient udpClientHandler, onMessage udpMessageHandler) (err error) {
	var (
		group       sync.WaitGroup
		errOnce     sync.Once
		errs        chan error    = make(chan error, 1)
		done        chan struct{} = make(chan struct{})
		workerCount int           = len(conns)
	)

	group.Add(workerCount)

	for _, conn := range conns {
		var conn *net.UDPConn = conn

		go func() {
			var workerErr error

			defer group.Done()

			if workerErr = server.readUDPLoop(conn, onClient, onMessage); workerErr != nil {
				errOnce.Do(func() {
					errs <- workerErr
					var _ error = closeUDPConns(conns)
				})
			}
		}()
	}

	go func() {
		group.Wait()
		close(done)
	}()

	select {
	case err = <-errs:
		<-done
	case <-done:
		select {
		case err = <-errs:
		default:
		}
	}

	return
}

func udpSocketShardCount(config ServerConfig) (count int) {
	if !udpReusePortAvailable() {
		count = 1
		return
	}

	count = max(min(runtime.GOMAXPROCS(0), defaultUDPSocketShards), 1)
	return
}

func (server *Server) acceptUDPClient(client *ServerClient) {
	var handler ClientHandler = server.clientHandler()

	if handler != nil {
		handler(client)
	}
}

func (server *Server) dispatchUDPMessage(client *ServerClient, message []byte) {
	client.dispatchMessage(message)
}

func (server *Server) udpClientSnapshot(addr udpClientKey) (serverClient *ServerClient) {
	server.mutex.RLock()
	serverClient = server.udpClients[addr]
	server.mutex.RUnlock()
	return
}

func (server *Server) udpClientFor(conn *net.UDPConn, addr udpClientKey) (serverClient *ServerClient, created bool) {
	var key udpClientKey = addr

	server.mutex.RLock()
	serverClient = server.udpClients[key]
	server.mutex.RUnlock()

	if serverClient != nil {
		return
	}

	server.mutex.Lock()
	defer server.mutex.Unlock()

	if serverClient = server.udpClients[key]; serverClient != nil {
		return
	}

	server.clientIDAccumulator++

	serverClient = &ServerClient{
		id: server.clientIDAccumulator,
		Client: newUDPVirtualClient(
			clientConfigFromServer(server.config),
			conn,
			addr,
			&server.udpWriter,
			func() {
				server.removeUDPClient(key)
			},
		),
	}

	server.clients[serverClient.id] = serverClient
	server.udpClients[key] = serverClient
	created = true
	return
}

func (server *Server) removeUDPClient(key udpClientKey) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	if client := server.udpClients[key]; client != nil {
		delete(server.clients, client.id)
	}

	delete(server.udpClients, key)
}

func (server *Server) setUDPConns(conns []*net.UDPConn) (err error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	if server.udpConn != nil || server.listener != nil {
		err = ErrServerAlreadyListening
		return
	}

	if len(conns) == 0 || conns[0] == nil {
		err = fmt.Errorf("%w: udp listen returned no sockets", ErrProtocolUnsupported)
		return
	}

	server.udpConn = conns[0]
	server.udpConns = append([]*net.UDPConn{}, conns...)
	return
}

func newUDPVirtualClient(
	config ClientConfig,
	conn *net.UDPConn,
	addr udpClientKey,
	writer *sync.Mutex,
	closeHook func(),
) (client *Client) {
	client = newClient(config, nil)
	client.udpConn = conn
	client.udpAddr = net.UDPAddrFromAddrPort(addr)
	client.udpAddrPort = addr
	client.udpWriter = writer
	client.closeHook = closeHook
	client.ownsConn = false
	return
}

func (client *Client) setUDPConnection(
	conn *net.UDPConn,
	addr *net.UDPAddr,
	ownsConn bool,
	writer *sync.Mutex,
	closeHook func(),
) {
	client.mutex.Lock()
	if conn == nil {
		client.conn = nil
	} else {
		client.conn = conn
	}
	client.udpConn = conn
	client.udpAddr = addr
	client.udpAddrPort = udpClientKey{}

	if addr != nil {
		client.udpAddrPort = addr.AddrPort()
	}

	client.ownsConn = ownsConn
	client.udpWriter = writer
	client.closeHook = closeHook
	client.mutex.Unlock()
}

func (client *Client) udpConnection() (conn *net.UDPConn) {
	client.mutex.RLock()
	conn = client.udpConn
	client.mutex.RUnlock()
	return
}

func (client *Client) udpRemoteAddr() (addr *net.UDPAddr) {
	client.mutex.RLock()
	addr = client.udpAddr
	client.mutex.RUnlock()
	return
}

func (client *Client) udpSendTarget() (udpConn *net.UDPConn, udpAddrPort udpClientKey, writer *sync.Mutex) {
	client.mutex.RLock()
	udpConn = client.udpConn
	udpAddrPort = client.udpAddrPort
	writer = client.udpWriter
	client.mutex.RUnlock()

	if writer == nil {
		writer = &client.writeMutex
	}

	return
}

func copyDatagram(data []byte) (copied []byte) {
	copied = make([]byte, len(data))
	copy(copied, data)
	return
}

func (client *Client) receivedDatagram(data []byte) (message []byte) {
	if client.config.UnsafeZeroCopy {
		message = data
		return
	}

	message = copyDatagram(data)
	return
}

func udpReadBufferSize(bufferSize int) (size int) {
	size = bufferSize + 1

	return
}
