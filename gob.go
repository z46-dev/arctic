package arctic

import (
	"bytes"
	"encoding/gob"
	"net"
)

func (client *GobClient[MessageType]) Connect() (err error) {
	var conn net.Conn

	if err = requireGobTypeRegistered[MessageType](); err != nil {
		return
	}

	if err = validateProtocol(client.config.Protocol); err != nil {
		return
	}

	if client.config.Protocol == ProtocolUDP {
		err = client.connectUDPGob()
		return
	}

	if conn, err = dialTCP(client.config); err != nil {
		return
	}

	client.setConnection(conn)
	client.setCodec(conn)
	go client.readMessages()
	return
}

func (client *GobClient[MessageType]) Send(message MessageType) (err error) {
	var conn net.Conn = client.connection()
	var encoder *gob.Encoder = client.encoderSnapshot()
	var hasDeadline bool = client.config.Timeout > 0

	if client.config.Protocol == ProtocolUDP {
		err = client.sendUDPGob(message)
		return
	}

	if conn == nil || encoder == nil {
		err = ErrClientNotConnected
		return
	}

	client.writeMutex.Lock()
	defer client.writeMutex.Unlock()

	if hasDeadline {
		if err = applyWriteDeadline(conn, client.config.Timeout); err != nil {
			return
		}
	}

	if err = encoder.Encode(message); err != nil {
		client.handleError(err)
	}

	if hasDeadline {
		clearWriteDeadline(conn)
	}

	return
}

func (client *GobClient[MessageType]) OnMessage(handler GobMessageHandler[MessageType]) {
	client.mutex.Lock()
	client.onMessage = handler
	client.mutex.Unlock()
}

func (client *GobClient[MessageType]) Use(middleware ...GobMiddleware[MessageType]) {
	client.mutex.Lock()
	client.middleware = append(client.middleware, middleware...)
	client.mutex.Unlock()
}

func (serverClient *GobServerClient[MessageType]) ID() (id int) {
	id = serverClient.id
	return
}

func (server *GobServer[MessageType]) Listen() (err error) {
	if server.config.Protocol == ProtocolUDP {
		err = server.Server.listenUDP(server.acceptUDPClient, server.dispatchUDPMessage)
		return
	}

	err = server.Server.listenTCP(server.acceptClient)
	return
}

func (server *GobServer[MessageType]) Close() (err error) {
	err = server.Server.Close()
	server.clearClients()
	return
}

func (server *GobServer[MessageType]) OnClient(handler GobClientHandler[MessageType]) {
	server.mutex.Lock()
	server.onClient = handler
	server.mutex.Unlock()
}

func (client *GobClient[MessageType]) setCodec(conn net.Conn) {
	client.mutex.Lock()
	client.encoder = gob.NewEncoder(conn)
	client.decoder = gob.NewDecoder(conn)
	client.mutex.Unlock()
}

func (client *GobClient[MessageType]) encoderSnapshot() (encoder *gob.Encoder) {
	client.mutex.RLock()
	encoder = client.encoder
	client.mutex.RUnlock()
	return
}

func (client *GobClient[MessageType]) decoderSnapshot() (decoder *gob.Decoder) {
	client.mutex.RLock()
	decoder = client.decoder
	client.mutex.RUnlock()
	return
}

func (client *GobClient[MessageType]) readMessages() {
	var decoder *gob.Decoder = client.decoderSnapshot()

	defer client.Close()

	for decoder != nil {
		var message MessageType
		var err error

		if err = decoder.Decode(&message); err != nil {
			client.handleError(err)
			return
		}

		client.dispatchMessage(message)
	}
}

func (client *GobClient[MessageType]) dispatchMessage(message MessageType) {
	var handler GobMessageHandler[MessageType]
	var middleware []GobMiddleware[MessageType]
	var context *GobMessageContext[MessageType]
	var index int
	var next GobNext[MessageType]
	var err error

	handler, middleware = client.messagePipeline()

	if len(middleware) == 0 {
		if handler != nil {
			handler(message)
		}

		return
	}

	context = &GobMessageContext[MessageType]{
		Client:  client,
		Message: message,
	}

	next = func(context *GobMessageContext[MessageType]) (err error) {
		if index == len(middleware) {
			if handler != nil {
				handler(context.Message)
			}

			return
		}

		var current GobMiddleware[MessageType] = middleware[index]
		index++
		err = current(context, next)
		return
	}

	if err = next(context); err != nil {
		client.handleError(err)
	}
}

func (client *GobClient[MessageType]) messagePipeline() (handler GobMessageHandler[MessageType], middleware []GobMiddleware[MessageType]) {
	client.mutex.RLock()
	handler = client.onMessage

	if len(client.middleware) > 0 {
		middleware = append([]GobMiddleware[MessageType]{}, client.middleware...)
	}

	client.mutex.RUnlock()
	return
}

func (server *GobServer[MessageType]) acceptClient(conn net.Conn) {
	var raw *ServerClient = server.Server.newServerClient(conn)
	var client *GobServerClient[MessageType] = &GobServerClient[MessageType]{
		id: raw.id,
		GobClient: &GobClient[MessageType]{
			Client:  raw.Client,
			encoder: gob.NewEncoder(conn),
			decoder: gob.NewDecoder(conn),
		},
	}
	var handler GobClientHandler[MessageType] = server.clientHandler()

	server.addClient(client)
	defer server.Server.removeClient(client.id)
	defer server.removeClient(client.id)

	if handler != nil {
		handler(client)
	}

	client.readMessages()
}

func (server *GobServer[MessageType]) addClient(client *GobServerClient[MessageType]) {
	server.mutex.Lock()
	server.gobClients[client.id] = client
	server.mutex.Unlock()
}

func (server *GobServer[MessageType]) removeClient(id int) {
	server.mutex.Lock()
	delete(server.gobClients, id)
	server.mutex.Unlock()
}

func (server *GobServer[MessageType]) clearClients() {
	server.mutex.Lock()
	server.gobClients = map[int]*GobServerClient[MessageType]{}
	server.mutex.Unlock()
}

func (server *GobServer[MessageType]) clientHandler() (handler GobClientHandler[MessageType]) {
	server.mutex.RLock()
	handler = server.onClient
	server.mutex.RUnlock()
	return
}

func (client *GobClient[MessageType]) connectUDPGob() (err error) {
	client.Client.OnMessage(func(data []byte) {
		var (
			message MessageType
			err     error
		)

		if message, err = decodeGobDatagram[MessageType](data); err != nil {
			client.handleError(err)
			return
		}

		client.dispatchMessage(message)
	})

	err = client.Client.connectUDP()
	return
}

func (client *GobClient[MessageType]) sendUDPGob(message MessageType) (err error) {
	var encoded []byte

	if encoded, err = encodeGobDatagram(message); err != nil {
		client.handleError(err)
		return
	}

	err = client.Client.Send(encoded)
	return
}

func (server *GobServer[MessageType]) acceptUDPClient(raw *ServerClient) {
	var client *GobServerClient[MessageType] = server.gobClientFor(raw)
	var handler GobClientHandler[MessageType] = server.clientHandler()

	if handler != nil {
		handler(client)
	}
}

func (server *GobServer[MessageType]) dispatchUDPMessage(raw *ServerClient, data []byte) {
	var (
		client  *GobServerClient[MessageType] = server.gobClientFor(raw)
		message MessageType
		err     error
	)

	if message, err = decodeGobDatagram[MessageType](data); err != nil {
		client.handleError(err)
		return
	}

	client.dispatchMessage(message)
}

func (server *GobServer[MessageType]) gobClientFor(raw *ServerClient) (client *GobServerClient[MessageType]) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	if client = server.gobClients[raw.id]; client != nil {
		return
	}

	client = &GobServerClient[MessageType]{
		id: raw.id,
		GobClient: &GobClient[MessageType]{
			Client: raw.Client,
		},
	}

	server.gobClients[client.id] = client
	return
}

func encodeGobDatagram[MessageType any](message MessageType) (data []byte, err error) {
	var buffer bytes.Buffer

	if err = gob.NewEncoder(&buffer).Encode(message); err != nil {
		return
	}

	data = buffer.Bytes()
	return
}

func decodeGobDatagram[MessageType any](data []byte) (message MessageType, err error) {
	err = gob.NewDecoder(bytes.NewReader(data)).Decode(&message)
	return
}
