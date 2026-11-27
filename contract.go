package arctic

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrContractCallTimeout    error = errors.New("arctic: contract call timed out")
	ErrContractFactoryMissing error = errors.New("arctic: contract factory is nil")
	ErrContractMessageDropped error = errors.New("arctic: contract message dropped by middleware")
	ErrContractStreamClosed   error = errors.New("arctic: contract stream is closed")
	ErrContractStreamStarted  error = errors.New("arctic: contract stream context was already used")
	ErrContractStreamBacklog  error = errors.New("arctic: contract stream handler queue is full")
)

type (
	ContractMessageType         uint16
	ContractMessagePhase        uint8
	ContractMiddlewareAction    uint8
	contractMiddlewareDirection uint8

	ContractMessage struct {
		Type  ContractMessageType
		Phase ContractMessagePhase
		Body  any
	}

	ContractMiddlewareContext struct {
		context.Context
		closeAfterResponse *bool
	}

	ContractMiddleware func(ctx *ContractMiddlewareContext, message *ContractMessage) (action ContractMiddlewareAction, response any, err error)

	contractFrame struct {
		ID        uint64
		Kind      string
		Method    string
		Type      ContractMessageType
		ScopeID   uint64
		StreamID  uint64
		Direction string
		Deadline  int64
		Payload   []byte
		Error     string
	}

	contractHandler    func(*ContractContext, []byte, ContractMessageType) ([]byte, error)
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
		client            *Client
		timeout           time.Duration
		nextID            atomic.Uint64
		mutex             sync.Mutex
		pending           map[uint64]chan contractFrame
		handlers          map[contractHandlerKey]contractHandler
		deferred          map[contractHandlerKey][]contractFrame
		inbound, outbound []ContractMiddleware
		active            map[uint64]activeContractCall
		streams           map[uint64]*contractStreamState
	}
)

const (
	ContractMessageRequest ContractMessagePhase = iota + 1
	ContractMessageResponse
)

const (
	ContractMiddlewareContinue ContractMiddlewareAction = iota
	ContractMiddlewareDrop
	ContractMiddlewareRespond
)

const (
	contractMiddlewareInbound contractMiddlewareDirection = iota
	contractMiddlewareOutbound
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
	response, err = contractCall[Request, Response](ctx, peer, 0, 0, method, request, nil)
	return
}

// ContractCallScoped sends a typed request from a nested contract scope.
func ContractCallScoped[Request, Response any](ctx context.Context, peer *ContractPeer, scopeID uint64, method string, request Request) (response Response, err error) {
	response, err = ContractCallTypedScoped[Request, Response](ctx, peer, scopeID, 0, method, request)
	return
}

// ContractCallTypedScoped sends a typed request with its generated message identifier.
func ContractCallTypedScoped[Request, Response any](ctx context.Context, peer *ContractPeer, scopeID uint64, messageType ContractMessageType, method string, request Request) (response Response, err error) {
	response, err = contractCall[Request, Response](ctx, peer, scopeID, messageType, method, request, nil)
	return
}

// ContractStreamingCall opens a duplex stream and waits for its final response.
func ContractStreamingCall[Request, Response any](ctx *ContractContext, method string, request Request) (response Response, err error) {
	if ctx == nil || ctx.peer == nil || ctx.stream == nil {
		err = errors.New("arctic: invalid contract streaming context")
		return
	}

	response, err = ContractStreamingCallTyped[Request, Response](ctx, 0, method, request)
	return
}

// ContractStreamingCallTyped opens a stream with its generated message identifier.
func ContractStreamingCallTyped[Request, Response any](ctx *ContractContext, messageType ContractMessageType, method string, request Request) (response Response, err error) {
	if ctx == nil || ctx.peer == nil || ctx.stream == nil {
		err = errors.New("arctic: invalid contract streaming context")
		return
	}

	response, err = contractCall[Request, Response](ctx, ctx.peer, ctx.stream.parentScopeID, messageType, method, request, ctx.stream)
	return
}

func contractCall[Request, Response any](ctx context.Context, peer *ContractPeer, scopeID uint64, messageType ContractMessageType, method string, request Request, stream *contractStreamState) (response Response, err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var (
		callContext        context.Context = ctx
		cancel             context.CancelFunc
		closeAfterResponse bool
	)

	defer func() {
		if closeAfterResponse {
			if closeErr := peer.client.Close(); err == nil {
				err = closeErr
			}
		}
	}()

	if peer.timeout > 0 {
		callContext, cancel = context.WithTimeout(ctx, peer.timeout)
		defer cancel()
	}

	if err = callContext.Err(); err != nil {
		return
	}

	var (
		message            *ContractMessage = &ContractMessage{Type: messageType, Phase: ContractMessageRequest, Body: request}
		action             ContractMiddlewareAction
		middlewareResponse any
	)

	if action, middlewareResponse, err = peer.runMiddleware(contractMiddlewareOutbound, callContext, message, &closeAfterResponse); err != nil {
		return
	}

	switch action {
	case ContractMiddlewareDrop:
		err = ErrContractMessageDropped
		return
	case ContractMiddlewareRespond:
		var ok bool
		if response, ok = middlewareResponse.(Response); !ok {
			err = fmt.Errorf("arctic: middleware response for %s has type %T", method, middlewareResponse)
		}

		return
	case ContractMiddlewareContinue:
		var ok bool
		if request, ok = message.Body.(Request); !ok {
			err = fmt.Errorf("arctic: middleware request for %s has type %T", method, message.Body)
			return
		}
	default:
		err = fmt.Errorf("arctic: invalid contract middleware action %d", action)
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
		Type:     messageType,
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
		if frame.Error == ErrContractMessageDropped.Error() {
			err = ErrContractMessageDropped
		} else {
			err = errors.New(frame.Error)
		}

		return
	}

	if frame.Type != messageType {
		err = fmt.Errorf("arctic: response for %s has message type %d, expected %d", method, frame.Type, messageType)
		return
	}

	err = decodeContractValue(frame.Payload, &response)
	if err != nil {
		return
	}

	message = &ContractMessage{Type: messageType, Phase: ContractMessageResponse, Body: response}
	if action, _, err = peer.runMiddleware(contractMiddlewareInbound, callContext, message, &closeAfterResponse); err != nil {
		return
	}

	switch action {
	case ContractMiddlewareContinue:
		var ok bool
		if response, ok = message.Body.(Response); !ok {
			err = fmt.Errorf("arctic: middleware response for %s has type %T", method, message.Body)
		}
	case ContractMiddlewareDrop:
		err = ErrContractMessageDropped
	case ContractMiddlewareRespond:
		err = errors.New("arctic: middleware cannot respond to a contract response")
	default:
		err = fmt.Errorf("arctic: invalid contract middleware action %d", action)
	}

	return
}

// ContractHandle registers a typed handler for unary requests received by this peer.
func ContractHandle[Request, Response any](peer *ContractPeer, method string, handler func(*ContractContext, Request) Response) {
	ContractHandleScoped(peer, 0, method, handler)
}

// ContractHandleScoped registers a typed handler in a nested contract scope.
func ContractHandleScoped[Request, Response any](peer *ContractPeer, scopeID uint64, method string, handler func(*ContractContext, Request) Response) {
	ContractHandleTypedScoped(peer, scopeID, 0, method, handler)
}

// ContractHandleTypedScoped registers a typed handler with its generated message identifier.
func ContractHandleTypedScoped[Request, Response any](peer *ContractPeer, scopeID uint64, messageType ContractMessageType, method string, handler func(*ContractContext, Request) Response) {
	registerContractHandler(peer, scopeID, method, makeContractHandler(messageType, handler))
}

// ContractStreamingHandle registers a typed handler for streaming requests.
func ContractStreamingHandle[Request, Response any](peer *ContractPeer, method string, handler func(*ContractContext, Request) Response) {
	ContractStreamingHandleScoped(peer, 0, method, handler)
}

// ContractStreamingHandleScoped registers a stream-opening handler in a nested contract scope.
func ContractStreamingHandleScoped[Request, Response any](peer *ContractPeer, scopeID uint64, method string, handler func(*ContractContext, Request) Response) {
	ContractStreamingHandleTypedScoped(peer, scopeID, 0, method, handler)
}

// ContractStreamingHandleTypedScoped registers a stream handler with its generated message identifier.
func ContractStreamingHandleTypedScoped[Request, Response any](peer *ContractPeer, scopeID uint64, messageType ContractMessageType, method string, handler func(*ContractContext, Request) Response) {
	registerContractHandler(peer, scopeID, method, makeContractHandler(messageType, handler))
}

// AddInboundMiddleware appends middleware for messages received by this peer.
func (peer *ContractPeer) AddInboundMiddleware(middleware ContractMiddleware) {
	if middleware == nil {
		return
	}

	peer.mutex.Lock()
	peer.inbound = append(peer.inbound, middleware)
	peer.mutex.Unlock()
}

// AddOutboundMiddleware appends middleware for messages sent by this peer.
func (peer *ContractPeer) AddOutboundMiddleware(middleware ContractMiddleware) {
	if middleware == nil {
		return
	}

	peer.mutex.Lock()
	peer.outbound = append(peer.outbound, middleware)
	peer.mutex.Unlock()
}

func (peer *ContractPeer) runMiddleware(direction contractMiddlewareDirection, ctx context.Context, message *ContractMessage, closeAfterResponse *bool) (action ContractMiddlewareAction, response any, err error) {
	var (
		messageType ContractMessageType  = message.Type
		phase       ContractMessagePhase = message.Phase
		middleware  []ContractMiddleware
	)

	peer.mutex.Lock()
	middleware = peer.outbound
	if direction == contractMiddlewareInbound {
		middleware = peer.inbound
	}

	middleware = append([]ContractMiddleware{}, middleware...)
	peer.mutex.Unlock()

	var middlewareContext *ContractMiddlewareContext = &ContractMiddlewareContext{
		Context:            ctx,
		closeAfterResponse: closeAfterResponse,
	}
	
	for _, apply := range middleware {
		if action, response, err = apply(middlewareContext, message); err != nil {
			return
		}

		if message.Type != messageType || message.Phase != phase {
			err = errors.New("arctic: middleware cannot change contract message type or phase")
			return
		}

		if action != ContractMiddlewareContinue {
			return
		}
	}

	return
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

// CloseAfterResponse closes the peer connection after the associated call completes.
func (ctx *ContractMiddlewareContext) CloseAfterResponse() {
	if ctx != nil && ctx.closeAfterResponse != nil {
		*ctx.closeAfterResponse = true
	}
}

func makeContractHandler[Request, Response any](messageType ContractMessageType, handler func(*ContractContext, Request) Response) (registered contractHandler) {
	registered = func(ctx *ContractContext, payload []byte, incomingType ContractMessageType) (encoded []byte, err error) {
		if incomingType != messageType {
			err = fmt.Errorf("arctic: request message type %d does not match registered type %d", incomingType, messageType)
			return
		}

		var request Request
		if err = decodeContractValue(payload, &request); err != nil {
			return
		}

		var (
			message  *ContractMessage = &ContractMessage{Type: incomingType, Phase: ContractMessageRequest, Body: request}
			action   ContractMiddlewareAction
			result   any
			response Response
		)

		if action, result, err = ctx.peer.runMiddleware(contractMiddlewareInbound, ctx, message, &ctx.closeAfterResponse); err != nil {
			return
		}

		switch action {
		case ContractMiddlewareContinue:
			var ok bool
			if request, ok = message.Body.(Request); !ok {
				err = fmt.Errorf("arctic: middleware request has type %T", message.Body)
				return
			}

			response = handler(ctx, request)
		case ContractMiddlewareRespond:
			var ok bool
			if response, ok = result.(Response); !ok {
				err = fmt.Errorf("arctic: middleware response has type %T", result)
				return
			}
		case ContractMiddlewareDrop:
			err = ErrContractMessageDropped
			return
		default:
			err = fmt.Errorf("arctic: invalid contract middleware action %d", action)
			return
		}

		message = &ContractMessage{Type: incomingType, Phase: ContractMessageResponse, Body: response}
		if action, _, err = ctx.peer.runMiddleware(contractMiddlewareOutbound, ctx, message, &ctx.closeAfterResponse); err != nil {
			return
		}

		switch action {
		case ContractMiddlewareContinue:
			var ok bool
			if response, ok = message.Body.(Response); !ok {
				err = fmt.Errorf("arctic: middleware response has type %T", message.Body)
				return
			}
		case ContractMiddlewareDrop:
			err = ErrContractMessageDropped
			return
		case ContractMiddlewareRespond:
			err = errors.New("arctic: middleware cannot respond to a contract response")
			return
		default:
			err = fmt.Errorf("arctic: invalid contract middleware action %d", action)
			return
		}

		encoded, err = encodeContractValue(response)
		return
	}

	return
}
