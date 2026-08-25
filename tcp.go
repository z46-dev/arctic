package arctic

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const tcpBufferedFramePayloadSize int = defaultBufferSize

// Connect opens the client's configured transport and starts receiving messages.
func (client *Client) Connect() (err error) {
	var conn net.Conn

	if err = validateProtocol(client.config.Protocol); err != nil {
		return
	}

	if client.config.Protocol == ProtocolUDP {
		err = client.connectUDP()
		return
	}

	if client.config.Protocol == ProtocolWebSocket {
		if conn, err = dialWebSocket(client.config); err != nil {
			return
		}
	} else if conn, err = dialTCP(client.config); err != nil {
		return
	}

	client.setConnection(conn)

	if err = client.sendMetadataHandshake(); err != nil {
		var _ error = conn.Close()
		client.setConnection(nil)
		return
	}

	go client.readMessages()
	return
}

// Send transmits one raw message to the remote peer.
func (client *Client) Send(message []byte) (err error) {
	var conn net.Conn

	if client.config.Protocol == ProtocolUDP {
		err = client.sendUDP(message)
		return
	}

	conn = client.connection()

	if err = validateFrameSize(message, client.config.BufferSize); err != nil {
		return
	}

	if conn == nil {
		err = ErrClientNotConnected
		return
	}

	client.writeMutex.Lock()
	defer client.writeMutex.Unlock()

	var hasDeadline bool = client.config.Timeout > 0

	if hasDeadline {
		if err = applyWriteDeadline(conn, client.config.Timeout); err != nil {
			return
		}
	}

	if err = client.writeTCPFrame(conn, message); err != nil {
		client.handleError(err)
	}

	if hasDeadline {
		clearWriteDeadline(conn)
	}

	return
}

// Close releases the client connection and invokes its close handler once.
func (client *Client) Close() (err error) {
	var closeErr error

	client.closeOnce.Do(func() {
		var conn net.Conn
		var handler CloseHandler
		var hook func()
		var ownsConn bool

		client.mutex.Lock()
		conn = client.conn
		client.conn = nil
		handler = client.onClose
		hook = client.closeHook
		ownsConn = client.ownsConn
		close(client.done)
		client.mutex.Unlock()

		if conn != nil && ownsConn {
			closeErr = conn.Close()
		}

		if hook != nil {
			hook()
		}

		if handler != nil {
			handler()
		}
	})

	err = closeErr
	return
}

// OnMessage sets the client's raw message handler.
func (client *Client) OnMessage(handler MessageHandler) {
	client.mutex.Lock()
	client.onMessage = handler
	client.mutex.Unlock()
}

// OnClose sets the client's close handler.
func (client *Client) OnClose(handler CloseHandler) {
	client.mutex.Lock()
	client.onClose = handler
	client.mutex.Unlock()
}

// OnError sets the client's asynchronous error handler.
func (client *Client) OnError(handler ErrorHandler) {
	client.mutex.Lock()
	client.onError = handler
	client.mutex.Unlock()
}

// Use appends middleware to the client's raw message pipeline.
func (client *Client) Use(middleware ...Middleware) {
	client.mutex.Lock()
	client.middleware = append(client.middleware, middleware...)
	client.mutex.Unlock()
}

// RemoteAddr returns the remote transport address when connected.
func (client *Client) RemoteAddr() (addr net.Addr) {
	var conn net.Conn = client.connection()
	var udpAddr *net.UDPAddr = client.udpRemoteAddr()

	if udpAddr != nil {
		addr = udpAddr
	} else if conn != nil {
		addr = conn.RemoteAddr()
	}

	return
}

// LocalAddr returns the local transport address when connected.
func (client *Client) LocalAddr() (addr net.Addr) {
	var conn net.Conn = client.connection()
	var udpConn *net.UDPConn = client.udpConnection()

	if udpConn != nil {
		addr = udpConn.LocalAddr()
	} else if conn != nil {
		addr = conn.LocalAddr()
	}

	return
}

// ID returns the server-assigned client identifier.
func (serverClient *ServerClient) ID() (id int) {
	id = serverClient.id
	return
}

// Listen starts the primary listener and every attached sidecar.
func (server *Server) Listen() (err error) {
	if server.config.Protocol == ProtocolUDP {
		err = server.listenWithSidecars(
			func(conn net.Conn) { server.acceptClientWithProtocol(conn, ProtocolWebSocket) },
			func() error { return server.listenUDP(server.acceptUDPClient, server.dispatchUDPMessage) },
		)
		return
	}

	err = server.listenWithSidecars(
		func(conn net.Conn) { server.acceptClientWithProtocol(conn, ProtocolWebSocket) },
		func() error { return server.listenTCP(server.acceptClient) },
	)
	return
}

// Close stops all listeners and connected clients.
func (server *Server) Close() (err error) {
	var closeErr error

	server.closeOnce.Do(func() {
		var clients []*ServerClient
		var listener net.Listener
		var udpConns []*net.UDPConn
		var handler CloseHandler
		var sidecars []*WebSocketServer

		server.mutex.Lock()
		listener = server.listener
		server.listener = nil
		udpConns = append([]*net.UDPConn{}, server.udpConns...)
		server.udpConn = nil
		server.udpConns = nil
		handler = server.onClose
		clients = make([]*ServerClient, 0, len(server.clients))
		sidecars = append([]*WebSocketServer{}, server.webSocketSidecars...)

		for _, client := range server.clients {
			clients = append(clients, client)
		}

		close(server.done)
		server.mutex.Unlock()

		for _, sidecar := range sidecars {
			if sidecarErr := sidecar.close(); closeErr == nil && sidecarErr != nil {
				closeErr = sidecarErr
			}
		}

		if listener != nil {
			closeErr = listener.Close()
		}

		for _, udpConn := range udpConns {
			var err error

			if udpConn == nil {
				continue
			}

			if err = udpConn.Close(); closeErr == nil && err != nil {
				closeErr = err
			}
		}

		for _, client := range clients {
			var err error

			if err = client.Close(); closeErr == nil && err != nil {
				closeErr = err
			}
		}

		if handler != nil {
			handler()
		}
	})

	err = closeErr
	return
}

func (server *Server) listenWithSidecars(webSocketAccept func(net.Conn), listen func() error) (err error) {
	var started []*WebSocketServer

	server.mutex.Lock()
	if server.listening {
		server.mutex.Unlock()
		return ErrServerAlreadyListening
	}
	server.listening = true
	server.listenErr = nil
	var sidecars []*WebSocketServer = append([]*WebSocketServer{}, server.webSocketSidecars...)
	server.mutex.Unlock()

	for _, sidecar := range sidecars {
		if err = sidecar.start(server, webSocketAccept); err != nil {
			for _, active := range started {
				_ = active.close()
			}
			server.setNotListening()
			return
		}
		started = append(started, sidecar)
	}

	err = listen()
	if err != nil {
		var bound bool = server.hasBoundListener()
		for _, active := range started {
			_ = active.close()
		}
		if bound {
			_ = server.Close()
		} else {
			server.setNotListening()
		}
		return
	}
	err = server.listenerError()
	return
}

func (server *Server) hasBoundListener() (bound bool) {
	server.mutex.RLock()
	bound = server.listener != nil || len(server.udpConns) > 0
	server.mutex.RUnlock()
	return
}

func (server *Server) setNotListening() {
	server.mutex.Lock()
	server.listening = false
	server.mutex.Unlock()
}

func (server *Server) setListenerError(err error) {
	server.mutex.Lock()
	if server.listenErr == nil {
		server.listenErr = err
	}
	server.mutex.Unlock()
}

func (server *Server) listenerError() (err error) {
	server.mutex.RLock()
	err = server.listenErr
	server.mutex.RUnlock()
	return
}

// OnClient sets the handler invoked for newly discovered clients.
func (server *Server) OnClient(handler ClientHandler) {
	server.mutex.Lock()
	server.onClient = handler
	server.mutex.Unlock()
}

// OnClose sets the server shutdown handler.
func (server *Server) OnClose(handler CloseHandler) {
	server.mutex.Lock()
	server.onClose = handler
	server.mutex.Unlock()
}

// OnError sets the server's asynchronous error handler.
func (server *Server) OnError(handler ErrorHandler) {
	server.mutex.Lock()
	server.onError = handler
	server.mutex.Unlock()
}

// Addr returns the primary listener's bound address.
func (server *Server) Addr() (addr net.Addr) {
	server.mutex.RLock()

	if server.listener != nil {
		addr = server.listener.Addr()
	} else if server.udpConn != nil {
		addr = server.udpConn.LocalAddr()
	}

	server.mutex.RUnlock()
	return
}

func dialTCP(config ClientConfig) (conn net.Conn, err error) {
	var dialer net.Dialer

	if config.Timeout > 0 {
		dialer.Timeout = config.Timeout
	}

	conn, err = dialer.Dial(string(config.Protocol), config.ServerAddress)
	return
}

func (client *Client) connection() (conn net.Conn) {
	client.mutex.RLock()
	conn = client.conn
	client.mutex.RUnlock()
	return
}

func (client *Client) setConnection(conn net.Conn) {
	client.mutex.Lock()
	client.conn = conn
	client.mutex.Unlock()
}

func (client *Client) readMessages() {
	var conn net.Conn = client.connection()
	var reader io.Reader
	var readBuffer []byte

	defer client.Close()

	if conn != nil {
		reader = bufio.NewReaderSize(conn, tcpReadBufferSize(client.config.BufferSize))
	}

	client.readMessagesFrom(reader, &readBuffer)
}

func (client *Client) readMessagesFrom(reader io.Reader, readBuffer *[]byte) {
	for reader != nil {
		var message []byte
		var err error

		if message, err = readFrame(reader, client.config.BufferSize, client.config.UnsafeZeroCopy, readBuffer); err != nil {
			client.handleError(err)
			return
		}

		client.dispatchMessage(message)
	}
}

func (client *Client) dispatchMessage(message []byte) {
	var handler MessageHandler
	var middleware []Middleware
	var context *MessageContext
	var index int
	var next Next
	var err error

	handler, middleware = client.messagePipeline()

	if len(middleware) == 0 {
		if handler != nil {
			handler(message)
		}

		return
	}

	context = &MessageContext{
		Client:  client,
		Message: message,
	}

	next = func(context *MessageContext) (err error) {
		if index == len(middleware) {
			if handler != nil {
				handler(context.Message)
			}

			return
		}

		var current Middleware = middleware[index]
		index++
		err = current(context, next)
		return
	}

	if err = next(context); err != nil {
		client.handleError(err)
	}
}

func (client *Client) messagePipeline() (handler MessageHandler, middleware []Middleware) {
	client.mutex.RLock()
	handler = client.onMessage

	if len(client.middleware) > 0 {
		middleware = append([]Middleware{}, client.middleware...)
	}

	client.mutex.RUnlock()
	return
}

func (client *Client) errorHandler() (handler ErrorHandler) {
	client.mutex.RLock()
	handler = client.onError
	client.mutex.RUnlock()
	return
}

func (client *Client) handleError(err error) {
	var handler ErrorHandler

	if err == nil || isExpectedCloseError(err) {
		return
	}

	if handler = client.errorHandler(); handler != nil {
		handler(err)
	}
}

func (server *Server) listenTCP(accept func(net.Conn)) (err error) {
	var listener net.Listener
	var release func()

	if err = validateServerProtocol(server.config.Protocol); err != nil {
		return
	}
	if release, err = reserveEndpoint("tcp", server.config.BindAddress); err != nil {
		return
	}
	defer release()

	if listener, err = net.Listen(string(server.config.Protocol), server.config.BindAddress); err != nil {
		err = endpointListenError(string(server.config.Protocol), server.config.BindAddress, err)
		return
	}

	if err = server.setListener(listener); err != nil {
		var closeErr error = listener.Close()

		if closeErr != nil {
			err = fmt.Errorf("%w: %v", err, closeErr)
		}

		return
	}

	for {
		var conn net.Conn

		if conn, err = listener.Accept(); err != nil {
			if errors.Is(err, net.ErrClosed) {
				err = nil
			} else {
				server.handleError(err)
			}

			return
		}

		go accept(conn)
	}
}

func (server *Server) setListener(listener net.Listener) (err error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	if server.listener != nil || server.udpConn != nil {
		err = ErrServerAlreadyListening
		return
	}

	server.listener = listener
	return
}

func (server *Server) acceptClient(conn net.Conn) {
	server.acceptClientWithProtocol(conn, ProtocolTCP)
}

func (server *Server) acceptClientWithProtocol(conn net.Conn, protocol Protocol) {
	var client *ServerClient = server.newServerClientWithProtocol(conn, protocol)
	var reader *bufio.Reader = bufio.NewReaderSize(conn, tcpReadBufferSize(client.config.BufferSize))
	var handler ClientHandler
	var firstMessage []byte
	var hasFirstMessage bool
	var readBuffer []byte
	var err error

	defer server.removeClient(client.id)

	if firstMessage, hasFirstMessage, err = client.receiveMetadataHandshake(reader, &readBuffer); err != nil {
		server.handleError(err)
		var _ error = client.Close()
		return
	}

	handler = server.clientHandler()

	if handler != nil {
		handler(client)
	}

	if hasFirstMessage {
		client.dispatchMessage(firstMessage)
	}

	client.readMessagesFrom(reader, &readBuffer)
	var _ error = client.Close()
}

func (server *Server) newServerClient(conn net.Conn) (serverClient *ServerClient) {
	return server.newServerClientWithProtocol(conn, server.config.Protocol)
}

func (server *Server) newServerClientWithProtocol(conn net.Conn, protocol Protocol) (serverClient *ServerClient) {
	var config ClientConfig = clientConfigFromServer(server.config)
	config.Protocol = protocol

	server.mutex.Lock()
	server.clientIDAccumulator++

	serverClient = &ServerClient{
		id:     server.clientIDAccumulator,
		Client: newClient(config, conn),
	}

	server.clients[serverClient.id] = serverClient
	server.mutex.Unlock()
	return
}

func (server *Server) removeClient(id int) {
	server.mutex.Lock()
	delete(server.clients, id)
	server.mutex.Unlock()
}

func (server *Server) clientHandler() (handler ClientHandler) {
	server.mutex.RLock()
	handler = server.onClient
	server.mutex.RUnlock()
	return
}

func (server *Server) errorHandler() (handler ErrorHandler) {
	server.mutex.RLock()
	handler = server.onError
	server.mutex.RUnlock()
	return
}

func (server *Server) handleError(err error) {
	var handler ErrorHandler

	if err == nil || isExpectedCloseError(err) {
		return
	}

	if handler = server.errorHandler(); handler != nil {
		handler(err)
	}
}

func readFrame(
	reader io.Reader,
	bufferSize int,
	unsafeZeroCopy bool,
	readBuffer *[]byte,
) (message []byte, err error) {
	var header [frameHeaderSize]byte
	var length uint32

	if _, err = io.ReadFull(reader, header[:]); err != nil {
		return
	}

	length = binary.BigEndian.Uint32(header[:])

	if length > uint32(bufferSize) {
		err = errMessageTooLarge(int(length), bufferSize)
		return
	}

	if unsafeZeroCopy {
		if cap(*readBuffer) < int(length) {
			*readBuffer = make([]byte, bufferSize)
		}

		message = (*readBuffer)[:int(length)]
	} else {
		message = make([]byte, int(length))
	}

	if length > 0 {
		_, err = io.ReadFull(reader, message)
	}

	return
}

func (client *Client) writeTCPFrame(writer net.Conn, message []byte) (err error) {
	var (
		frame   []byte
		total   int = frameHeaderSize + len(message)
		written int
	)

	if len(message) > tcpBufferedFramePayloadSize {
		err = writeFrame(writer, message)
		return
	}

	if cap(client.writeBuffer) < total {
		client.writeBuffer = make([]byte, frameHeaderSize+tcpBufferedFramePayloadSize)
	}

	frame = client.writeBuffer[:total]
	binary.BigEndian.PutUint32(frame[:frameHeaderSize], uint32(len(message)))
	copy(frame[frameHeaderSize:], message)

	if written, err = writer.Write(frame); err != nil {
		return
	}

	if written != total {
		err = io.ErrShortWrite
	}

	return
}

func writeFrame(writer net.Conn, message []byte) (err error) {
	var header [frameHeaderSize]byte
	var buffers net.Buffers
	var written int64

	binary.BigEndian.PutUint32(header[:], uint32(len(message)))
	buffers = net.Buffers{header[:], message}

	if written, err = buffers.WriteTo(writer); err != nil {
		return
	}

	if written != int64(frameHeaderSize+len(message)) {
		err = io.ErrShortWrite
	}

	return
}

func tcpReadBufferSize(bufferSize int) (size int) {
	size = bufferSize + frameHeaderSize

	if size < defaultBufferSize {
		size = defaultBufferSize
	}

	return
}

func applyWriteDeadline(conn net.Conn, timeout time.Duration) (err error) {
	if timeout > 0 {
		err = conn.SetWriteDeadline(time.Now().Add(timeout))
	}

	return
}

func clearWriteDeadline(conn net.Conn) {
	var _ error = conn.SetWriteDeadline(time.Time{})
}
