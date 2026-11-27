package arctic

import (
	"context"
	"fmt"
	"time"
)

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

	switch frame.Kind {
	case "response":
		peer.mutex.Lock()
		var result chan contractFrame = peer.pending[frame.ID]
		peer.mutex.Unlock()

		if result != nil {
			result <- frame
		}

		if frame.StreamID != 0 {
			peer.closeStream(frame.StreamID)
		}
	case "cancel":
		peer.mutex.Lock()
		var activeCall activeContractCall = peer.active[frame.ID]
		peer.mutex.Unlock()

		if activeCall.cancel != nil {
			activeCall.cancel()
		}

		if frame.StreamID != 0 {
			peer.closeStream(frame.StreamID)
		}
	case "stream_open":
		peer.mutex.Lock()
		var stream *contractStreamState = peer.streams[frame.StreamID]
		peer.mutex.Unlock()

		if stream != nil {
			stream.open()
		}
	case "stream_data":
		if frame.Direction != contractRequestDirection && frame.Direction != contractResponseDirection {
			peer.client.handleError(fmt.Errorf("arctic: invalid contract stream direction %q", frame.Direction))
			return
		}

		peer.mutex.Lock()
		var stream *contractStreamState = peer.streams[frame.StreamID]
		peer.mutex.Unlock()

		if stream != nil {
			if !stream.deliver(frame.Direction, frame.Payload) {
				peer.client.handleError(ErrContractStreamBacklog)
				_ = peer.send(contractFrame{ID: frame.StreamID, Kind: "cancel", StreamID: frame.StreamID})
				peer.closeStream(frame.StreamID)
			}
		}
	case "stream_close":
		peer.mutex.Lock()
		var stream *contractStreamState = peer.streams[frame.StreamID]
		peer.mutex.Unlock()

		if stream != nil {
			if !stream.remoteClose(frame.Direction) {
				peer.client.handleError(fmt.Errorf("arctic: invalid contract stream direction %q", frame.Direction))
			}
		}
	case "request":
		peer.dispatchIncoming(frame)
	default:
		peer.client.handleError(fmt.Errorf("arctic: unknown contract frame kind %q", frame.Kind))
	}
}

func (peer *ContractPeer) dispatchIncoming(frame contractFrame) {
	peer.mutex.Lock()
	var key contractHandlerKey = contractHandlerKey{scopeID: frame.ScopeID, method: frame.Method}
	var handler contractHandler = peer.handlers[key]
	if handler == nil && frame.ScopeID != 0 {
		peer.deferred[key] = append(peer.deferred[key], frame)
		peer.mutex.Unlock()
		return
	}
	peer.mutex.Unlock()

	if handler == nil {
		_ = peer.send(contractFrame{ID: frame.ID, Kind: "response", Type: frame.Type, StreamID: frame.StreamID, Error: "unknown contract method: " + frame.Method})
		return
	}

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	if frame.Deadline == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithDeadline(context.Background(), time.Unix(0, frame.Deadline))
	}

	var contractStream *contractStreamState
	if frame.StreamID != 0 {
		contractStream = newContractStreamState(peer, frame.StreamID, frame.ScopeID, true)
		peer.mutex.Lock()
		peer.streams[frame.StreamID] = contractStream
		var deferredFrames []contractFrame
		for key, frames := range peer.deferred {
			if key.scopeID == frame.StreamID {
				deferredFrames = append(deferredFrames, frames...)
				delete(peer.deferred, key)
			}
		}
		peer.mutex.Unlock()
		for _, deferredFrame := range deferredFrames {
			peer.dispatchIncoming(deferredFrame)
		}
	}

	var handlerContext *ContractContext = &ContractContext{
		Context:         ctx,
		peer:            peer,
		stream:          contractStream,
		scopeID:         frame.ScopeID,
		streamInitiator: false,
	}

	peer.mutex.Lock()
	peer.active[frame.ID] = activeContractCall{scopeID: frame.ScopeID, cancel: cancel}
	peer.mutex.Unlock()

	if contractStream != nil {
		_ = peer.send(contractFrame{Kind: "stream_open", StreamID: frame.StreamID})
	}

	go peer.dispatchRequest(frame, handler, handlerContext, cancel)
}

func (peer *ContractPeer) dispatchRequest(frame contractFrame, handler contractHandler, ctx *ContractContext, cancel context.CancelFunc) {
	var (
		payload []byte
		err     error
	)

	if payload, err = handler(ctx, frame.Payload, frame.Type); err != nil {
		_ = peer.send(contractFrame{ID: frame.ID, Kind: "response", Type: frame.Type, StreamID: frame.StreamID, Error: err.Error()})
		peer.finishRequest(frame.ID, frame.StreamID, cancel)
		return
	}

	if ctx.Err() == nil {
		if err = peer.send(contractFrame{ID: frame.ID, Kind: "response", Type: frame.Type, StreamID: frame.StreamID, Payload: payload}); err != nil {
			peer.client.handleError(err)
		}
	}

	peer.finishRequest(frame.ID, frame.StreamID, cancel)
	if ctx.closeAfterResponse {
		if err = peer.client.Close(); err != nil {
			peer.client.handleError(err)
		}
	}
}

func (peer *ContractPeer) finishRequest(id uint64, streamID uint64, cancel context.CancelFunc) {
	cancel()
	peer.mutex.Lock()
	delete(peer.active, id)
	peer.mutex.Unlock()

	if streamID != 0 {
		peer.closeStream(streamID)
	}
}

func (peer *ContractPeer) closeStream(id uint64) {
	peer.mutex.Lock()
	var stream *contractStreamState = peer.streams[id]
	delete(peer.streams, id)
	var childStreams []uint64
	var cancellations []context.CancelFunc
	for childID, child := range peer.streams {
		if child.parentScopeID == id {
			childStreams = append(childStreams, childID)
		}
	}
	for key := range peer.deferred {
		if key.scopeID == id {
			delete(peer.deferred, key)
		}
	}
	for _, activeCall := range peer.active {
		if activeCall.scopeID == id {
			cancellations = append(cancellations, activeCall.cancel)
		}
	}
	peer.mutex.Unlock()

	if stream != nil {
		stream.closeAll()
	}
	for _, cancel := range cancellations {
		cancel()
	}
	for _, childID := range childStreams {
		peer.closeStream(childID)
	}
}

func (peer *ContractPeer) closeAllStreams() {
	peer.mutex.Lock()

	var streams []*contractStreamState
	for id, stream := range peer.streams {
		streams = append(streams, stream)
		delete(peer.streams, id)
	}

	peer.mutex.Unlock()
	for _, stream := range streams {
		stream.closeAll()
	}
}

func (peer *ContractPeer) cancelActive() {
	peer.mutex.Lock()

	var cancellations []context.CancelFunc
	for _, activeCall := range peer.active {
		cancellations = append(cancellations, activeCall.cancel)
	}

	peer.mutex.Unlock()
	for _, cancel := range cancellations {
		cancel()
	}
}
