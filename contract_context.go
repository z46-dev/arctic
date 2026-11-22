package arctic

import (
	"errors"
	"fmt"
)

// SendContractStreamData sends one typed stream message in the selected direction.
func SendContractStreamData[Message any](ctx *ContractContext, direction string, message Message) (err error) {
	if ctx == nil || ctx.stream == nil {
		err = errors.New("arctic: no contract stream is attached to this context")
		return
	}

	if err = ctx.stream.waitOpen(ctx); err != nil {
		return
	}

	var payload []byte
	if payload, err = encodeContractValue(message); err != nil {
		return
	}

	if err = ctx.stream.canSend(direction); err != nil {
		return
	}

	err = ctx.peer.send(contractFrame{Kind: "stream_data", StreamID: ctx.stream.id, Direction: direction, Payload: payload})
	return
}

// OnContractStreamData registers a handler for typed messages in one stream direction.
func OnContractStreamData[Message any](ctx *ContractContext, direction string, handler func(Message)) {
	if ctx == nil || ctx.stream == nil {
		return
	}

	ctx.stream.setHandler(direction, func(payload []byte) {
		var (
			message Message
			err     error
		)

		if err = decodeContractValue(payload, &message); err != nil {
			ctx.peer.client.handleError(fmt.Errorf("arctic: invalid contract stream message: %w", err))
			return
		}

		handler(message)
	})
}

// CloseStream closes this endpoint's sending half of the current stream.
func (ctx *ContractContext) CloseStream() (err error) {
	if ctx == nil || ctx.stream == nil {
		err = errors.New("arctic: no contract stream is attached to this context")
		return
	}

	var direction string = contractResponseDirection
	if ctx.streamInitiator {
		direction = contractRequestDirection
	}

	if err = ctx.stream.closeLocal(direction); err != nil {
		return
	}

	err = ctx.peer.send(contractFrame{Kind: "stream_close", StreamID: ctx.stream.id, Direction: direction})
	return
}

// OnStreamClose runs when the other endpoint closes its sending half or the stream ends.
func (ctx *ContractContext) OnStreamClose(handler func()) {
	if ctx == nil || ctx.stream == nil {
		return
	}

	ctx.stream.onClose(handler)
}

// CloseAfterResponse closes this peer after the current handler's response is sent.
func (ctx *ContractContext) CloseAfterResponse() {
	ctx.closeAfterResponse = true
}

// OnClient sets the handler invoked for each connected contract server client.
func (server *ContractServer[ContractType]) OnClient(handler func(*ContractServerClient[ContractType])) {
	server.mutex.Lock()
	server.onClient = handler
	server.mutex.Unlock()

	server.Server.OnClient(func(raw *ServerClient) {
		var (
			peer   *ContractPeer                       = NewContractPeer(raw.Client)
			client *ContractServerClient[ContractType] = &ContractServerClient[ContractType]{
				ServerClient: raw,
				Contract:     server.factory(peer),
			}
		)

		server.mutex.RLock()
		var currentHandler func(*ContractServerClient[ContractType]) = server.onClient
		server.mutex.RUnlock()

		if currentHandler != nil {
			currentHandler(client)
		}
	})
}
