package arctic

import (
	"bytes"
	"context"
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
		ID       uint64
		Kind     string
		Method   string
		Deadline int64
		Payload  []byte
		Error    string
	}

	contractHandler func(*ContractContext, []byte) ([]byte, error)

	// ContractContext is the context for one inbound contract handler call.
	ContractContext struct {
		context.Context
		closeAfterResponse bool
	}

	ContractPeer struct {
		client   *Client
		timeout  time.Duration
		nextID   atomic.Uint64
		mutex    sync.Mutex
		pending  map[uint64]chan contractFrame
		handlers map[string]contractHandler
		active   map[uint64]context.CancelFunc
	}
)

// NewContractPeer adds correlated request/reply dispatch to a raw Arctic client.
func NewContractPeer(client *Client) (peer *ContractPeer) {
	peer = &ContractPeer{
		client:   client,
		timeout:  client.config.Timeout,
		pending:  map[uint64]chan contractFrame{},
		handlers: map[string]contractHandler{},
		active:   map[uint64]context.CancelFunc{},
	}

	client.OnMessage(peer.onMessage)
	client.mutex.Lock()

	var previousCloseHook func() = client.closeHook
	client.closeHook = func() {
		if previousCloseHook != nil {
			previousCloseHook()
		}

		peer.cancelActive()
	}

	client.mutex.Unlock()
	return
}

// ContractCall sends a typed request and waits for its typed response.
func ContractCall[Request, Response any](ctx context.Context, peer *ContractPeer, method string, request Request) (response Response, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var (
		callContext context.Context = ctx
		cancel      context.CancelFunc
	)

	if peer.timeout > 0 {
		callContext, cancel = context.WithTimeout(ctx, peer.timeout)
		defer cancel()
	}

	if err = callContext.Err(); err != nil {
		return
	}

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

	defer func() {
		peer.mutex.Lock()
		delete(peer.pending, id)
		peer.mutex.Unlock()
	}()

	var deadline int64
	if deadlineTime, hasDeadline := callContext.Deadline(); hasDeadline {
		deadline = deadlineTime.UnixNano()
	}

	if err = peer.send(contractFrame{
		ID:       id,
		Kind:     "request",
		Method:   method,
		Deadline: deadline,
		Payload:  payload,
	}); err != nil {
		return
	}

	var frame contractFrame
	select {
	case frame = <-result:
	case <-callContext.Done():
		if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			err = ErrContractCallTimeout
		}

		_ = peer.send(contractFrame{ID: id, Kind: "cancel"})
		return
	}

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
func ContractHandle[Request, Response any](peer *ContractPeer, method string, handler func(*ContractContext, Request) Response) {
	peer.mutex.Lock()
	defer peer.mutex.Unlock()

	peer.handlers[method] = func(ctx *ContractContext, payload []byte) (encoded []byte, err error) {
		var request Request
		if err = decodeContractValue(payload, &request); err != nil {
			return
		}

		var response Response = handler(ctx, request)
		encoded, err = encodeContractValue(response)
		return
	}
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

	if frame.Kind == "cancel" {
		peer.mutex.Lock()
		var cancel context.CancelFunc = peer.active[frame.ID]
		peer.mutex.Unlock()

		if cancel != nil {
			cancel()
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

	var (
		ctx       context.Context
		cancel    context.CancelFunc
		ctxHandle *ContractContext
	)

	if frame.Deadline == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithDeadline(context.Background(), time.Unix(0, frame.Deadline))
	}

	ctxHandle = &ContractContext{Context: ctx}
	peer.mutex.Lock()
	peer.active[frame.ID] = cancel
	peer.mutex.Unlock()

	go peer.dispatchRequest(frame, handler, ctxHandle, cancel)
}

func (peer *ContractPeer) dispatchRequest(frame contractFrame, handler contractHandler, ctx *ContractContext, cancel context.CancelFunc) {
	var (
		payload []byte
		err     error
	)

	if payload, err = handler(ctx, frame.Payload); err != nil {
		cancel()
		peer.mutex.Lock()
		delete(peer.active, frame.ID)
		peer.mutex.Unlock()

		_ = peer.send(contractFrame{ID: frame.ID, Kind: "response", Error: err.Error()})
		return
	}

	if ctx.Err() == nil {
		if err = peer.send(contractFrame{ID: frame.ID, Kind: "response", Payload: payload}); err != nil {
			peer.client.handleError(err)
		}
	}

	cancel()
	peer.mutex.Lock()
	delete(peer.active, frame.ID)
	peer.mutex.Unlock()

	if ctx.closeAfterResponse {
		if err = peer.client.Close(); err != nil {
			peer.client.handleError(err)
		}
	}
}

// Cancel requests cancellation of active handlers when this peer closes.
func (peer *ContractPeer) cancelActive() {
	peer.mutex.Lock()

	var cancellations []context.CancelFunc
	for _, cancel := range peer.active {
		cancellations = append(cancellations, cancel)
	}

	peer.mutex.Unlock()

	for _, cancel := range cancellations {
		cancel()
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
