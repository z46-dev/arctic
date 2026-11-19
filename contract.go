package arctic

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrContractCallTimeout    error = errors.New("arctic: contract call timed out")
	ErrContractFactoryMissing error = errors.New("arctic: contract factory is nil")
)

type (
	contractFrame struct {
		ID      uint64
		Kind    string
		Method  string
		Payload []byte
		Error   string
	}

	contractHandler func([]byte) ([]byte, error)

	ContractPeer struct {
		client   *Client
		timeout  time.Duration
		nextID   atomic.Uint64
		mutex    sync.Mutex
		pending  map[uint64]chan contractFrame
		handlers map[string]contractHandler
	}
)

// NewContractPeer adds correlated request/reply dispatch to a raw Arctic client.
func NewContractPeer(client *Client) (peer *ContractPeer) {
	peer = &ContractPeer{
		client:   client,
		timeout:  client.config.Timeout,
		pending:  map[uint64]chan contractFrame{},
		handlers: map[string]contractHandler{},
	}

	client.OnMessage(peer.onMessage)
	return
}

// ContractCall sends a typed request and waits for its typed response.
func ContractCall[Request, Response any](peer *ContractPeer, method string, request Request) (response Response, err error) {
	var payload []byte
	if payload, err = encodeContractValue(request); err != nil {
		return
	}

	var (
		id     uint64             = peer.nextID.Add(1)
		result chan contractFrame = make(chan contractFrame, 1)
	)

	peer.mutex.Lock()
	peer.pending[id] = result
	peer.mutex.Unlock()

	if err = peer.send(contractFrame{
		ID:      id,
		Kind:    "request",
		Method:  method,
		Payload: payload,
	}); err != nil {
		peer.mutex.Lock()
		delete(peer.pending, id)
		peer.mutex.Unlock()
		return
	}

	var frame contractFrame
	if peer.timeout > 0 {
		select {
		case frame = <-result:
		case <-time.After(peer.timeout):
			err = ErrContractCallTimeout
		}
	} else {
		frame = <-result
	}

	peer.mutex.Lock()
	delete(peer.pending, id)
	peer.mutex.Unlock()

	if err != nil {
		return
	}

	if frame.Error != "" {
		err = errors.New(frame.Error)
		return
	}

	err = decodeContractValue(frame.Payload, &response)
	return
}

// ContractHandle registers a typed handler for requests received by this peer.
func ContractHandle[Request, Response any](peer *ContractPeer, method string, handler func(Request) Response) {
	peer.mutex.Lock()
	defer peer.mutex.Unlock()

	peer.handlers[method] = func(payload []byte) (encoded []byte, err error) {
		var request Request
		if err = decodeContractValue(payload, &request); err != nil {
			return
		}

		var response Response = handler(request)
		encoded, err = encodeContractValue(response)
		return
	}
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

func (peer *ContractPeer) send(frame contractFrame) (err error) {
	var encoded []byte
	if encoded, err = encodeContractValue(frame); err != nil {
		return
	}

	err = peer.client.Send(encoded)
	return
}

func (peer *ContractPeer) onMessage(message []byte) {
	var (
		frame contractFrame
		err   error
	)

	if err = decodeContractValue(message, &frame); err != nil {
		peer.client.handleError(fmt.Errorf("arctic: invalid contract frame: %w", err))
		return
	}

	if frame.Kind == "response" {
		peer.mutex.Lock()
		var result chan contractFrame = peer.pending[frame.ID]
		peer.mutex.Unlock()

		if result != nil {
			result <- frame
		}

		return
	}

	peer.mutex.Lock()
	var handler contractHandler = peer.handlers[frame.Method]
	peer.mutex.Unlock()

	if handler == nil {
		_ = peer.send(contractFrame{ID: frame.ID, Kind: "response", Error: "unknown contract method: " + frame.Method})
		return
	}

	var payload []byte
	if payload, err = handler(frame.Payload); err != nil {
		_ = peer.send(contractFrame{ID: frame.ID, Kind: "response", Error: err.Error()})
		return
	}

	if err = peer.send(contractFrame{ID: frame.ID, Kind: "response", Payload: payload}); err != nil {
		peer.client.handleError(err)
	}
}

func encodeContractValue(value any) (encoded []byte, err error) {
	var buffer bytes.Buffer
	if err = gob.NewEncoder(&buffer).Encode(value); err != nil {
		return
	}

	encoded = buffer.Bytes()
	return
}

func decodeContractValue(encoded []byte, value any) (err error) {
	err = gob.NewDecoder(bytes.NewReader(encoded)).Decode(value)
	return
}
