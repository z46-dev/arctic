package arctic

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrContractCallTimeout    error = errors.New("arctic: contract call timed out")
	ErrContractFactoryMissing error = errors.New("arctic: contract factory is nil")
	ErrContractStreamClosed   error = errors.New("arctic: contract stream is closed")
	ErrContractStreamStarted  error = errors.New("arctic: contract stream context was already used")
	ErrContractStreamBacklog  error = errors.New("arctic: contract stream handler queue is full")
)

type (
	contractFrame struct {
		ID        uint64
		Kind      string
		Method    string
		ScopeID   uint64
		StreamID  uint64
		Direction string
		Deadline  int64
		Payload   []byte
		Error     string
	}

	contractHandler    func(*ContractContext, []byte) ([]byte, error)
	contractHandlerKey struct {
		scopeID uint64
		method  string
	}
	activeContractCall struct {
		scopeID uint64
		cancel  context.CancelFunc
	}

	ContractContext struct {
		context.Context
		peer               *ContractPeer
		stream             *contractStreamState
		scopeID            uint64
		streamInitiator    bool
		closeAfterResponse bool
	}

	ContractPeer struct {
		client   *Client
		timeout  time.Duration
		nextID   atomic.Uint64
		mutex    sync.Mutex
		pending  map[uint64]chan contractFrame
		handlers map[contractHandlerKey]contractHandler
		deferred map[contractHandlerKey][]contractFrame
		active   map[uint64]activeContractCall
		streams  map[uint64]*contractStreamState
	}
)

const (
	contractRequestDirection  string = "request"
	contractResponseDirection string = "response"
	contractStreamQueueLimit  int    = 128
)

// NewContractPeer adds correlated request/reply dispatch to a raw Arctic client.
func NewContractPeer(client *Client) (peer *ContractPeer) {
	peer = &ContractPeer{
		client:   client,
		timeout:  client.config.Timeout,
		pending:  map[uint64]chan contractFrame{},
		handlers: map[contractHandlerKey]contractHandler{},
		deferred: map[contractHandlerKey][]contractFrame{},
		active:   map[uint64]activeContractCall{},
		streams:  map[uint64]*contractStreamState{},
	}

	client.OnMessage(peer.onMessage)
	client.mutex.Lock()

	var previousCloseHook func() = client.closeHook
	client.closeHook = func() {
		if previousCloseHook != nil {
			previousCloseHook()
		}

		peer.cancelActive()
		peer.closeAllStreams()
	}

	client.mutex.Unlock()
	return
}

// NewContractStreamingContext creates the initiator side of a typed duplex stream.
func NewContractStreamingContext(parent context.Context, peer *ContractPeer, parentScopeID uint64) (ctx *ContractContext) {
	if parent == nil {
		parent = context.Background()
	}

	var stream *contractStreamState = newContractStreamState(peer, peer.nextID.Add(1), parentScopeID, false)
	ctx = &ContractContext{
		Context:         parent,
		peer:            peer,
		stream:          stream,
		scopeID:         stream.id,
		streamInitiator: true,
	}

	return
}

// ContractCall sends a typed request and waits for its typed response.
func ContractCall[Request, Response any](ctx context.Context, peer *ContractPeer, method string, request Request) (response Response, err error) {
	response, err = contractCall[Request, Response](ctx, peer, 0, method, request, nil)
	return
}

// ContractCallScoped sends a typed request from a nested contract scope.
func ContractCallScoped[Request, Response any](ctx context.Context, peer *ContractPeer, scopeID uint64, method string, request Request) (response Response, err error) {
	response, err = contractCall[Request, Response](ctx, peer, scopeID, method, request, nil)
	return
}

// ContractStreamingCall opens a duplex stream and waits for its final response.
func ContractStreamingCall[Request, Response any](ctx *ContractContext, method string, request Request) (response Response, err error) {
	if ctx == nil || ctx.peer == nil || ctx.stream == nil {
		err = errors.New("arctic: invalid contract streaming context")
		return
	}

	response, err = contractCall[Request, Response](ctx, ctx.peer, ctx.stream.parentScopeID, method, request, ctx.stream)
	return
}

func contractCall[Request, Response any](ctx context.Context, peer *ContractPeer, scopeID uint64, method string, request Request, stream *contractStreamState) (response Response, err error) {
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
		id     uint64
		result chan contractFrame = make(chan contractFrame, 1)
	)

	if stream == nil {
		id = peer.nextID.Add(1)
	} else {
		if err = stream.begin(); err != nil {
			return
		}
		id = stream.id
	}

	peer.mutex.Lock()
	peer.pending[id] = result
	if stream != nil {
		peer.streams[stream.id] = stream
	}

	peer.mutex.Unlock()

	defer func() {
		peer.mutex.Lock()
		delete(peer.pending, id)
		peer.mutex.Unlock()

		if stream != nil {
			peer.closeStream(stream.id)
		}
	}()

	var deadline int64
	if deadlineTime, hasDeadline := callContext.Deadline(); hasDeadline {
		deadline = deadlineTime.UnixNano()
	}

	var streamID uint64
	if stream != nil {
		streamID = stream.id
	}

	if err = peer.send(contractFrame{
		ID:       id,
		Kind:     "request",
		Method:   method,
		ScopeID:  scopeID,
		StreamID: streamID,
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

		_ = peer.send(contractFrame{ID: id, Kind: "cancel", StreamID: streamID})
		return
	}

	if frame.Error != "" {
		err = errors.New(frame.Error)
		return
	}

	err = decodeContractValue(frame.Payload, &response)
	return
}

// ContractHandle registers a typed handler for unary requests received by this peer.
func ContractHandle[Request, Response any](peer *ContractPeer, method string, handler func(*ContractContext, Request) Response) {
	ContractHandleScoped(peer, 0, method, handler)
}

// ContractHandleScoped registers a typed handler in a nested contract scope.
func ContractHandleScoped[Request, Response any](peer *ContractPeer, scopeID uint64, method string, handler func(*ContractContext, Request) Response) {
	registerContractHandler(peer, scopeID, method, makeContractHandler(handler))
}

// ContractStreamingHandle registers a typed handler for streaming requests.
func ContractStreamingHandle[Request, Response any](peer *ContractPeer, method string, handler func(*ContractContext, Request) Response) {
	ContractStreamingHandleScoped(peer, 0, method, handler)
}

// ContractStreamingHandleScoped registers a stream-opening handler in a nested contract scope.
func ContractStreamingHandleScoped[Request, Response any](peer *ContractPeer, scopeID uint64, method string, handler func(*ContractContext, Request) Response) {
	registerContractHandler(peer, scopeID, method, makeContractHandler(handler))
}

func registerContractHandler(peer *ContractPeer, scopeID uint64, method string, handler contractHandler) {
	peer.mutex.Lock()
	var key contractHandlerKey = contractHandlerKey{scopeID: scopeID, method: method}
	peer.handlers[key] = handler
	var deferred []contractFrame = peer.deferred[key]
	delete(peer.deferred, key)
	peer.mutex.Unlock()
	for _, frame := range deferred {
		peer.dispatchIncoming(frame)
	}
}

// ScopeID returns the active contract scope for nested contract wrappers.
func (ctx *ContractContext) ScopeID() (scopeID uint64) {
	scopeID = ctx.scopeID
	if ctx.stream != nil {
		scopeID = ctx.stream.id
	}
	return
}

func makeContractHandler[Request, Response any](handler func(*ContractContext, Request) Response) (registered contractHandler) {
	registered = func(ctx *ContractContext, payload []byte) (encoded []byte, err error) {
		var request Request
		if err = decodeContractValue(payload, &request); err != nil {
			return
		}

		var response Response = handler(ctx, request)
		encoded, err = encodeContractValue(response)
		return
	}

	return
}
