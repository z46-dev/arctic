package arctic

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type (
	contractStreamState struct {
		peer          *ContractPeer
		id            uint64
		parentScopeID uint64
		opened        chan struct{}
		openOnce      sync.Once
		mutex         sync.Mutex
		closed        map[string]bool
		receivers     map[string]*contractStreamReceiver
		closeHooks    []func()
		closeNotified bool
		closeOnce     sync.Once
		started       bool
	}

	contractStreamReceiver struct {
		mutex    sync.Mutex
		ready    *sync.Cond
		handler  func([]byte)
		queue    [][]byte
		closed   bool
		started  bool
		finished bool
		onClose  []func()
	}
)

func newContractStreamState(peer *ContractPeer, id uint64, parentScopeID uint64, opened bool) (stream *contractStreamState) {
	stream = &contractStreamState{
		peer:          peer,
		id:            id,
		parentScopeID: parentScopeID,
		opened:        make(chan struct{}),
		closed:        map[string]bool{},
		receivers: map[string]*contractStreamReceiver{
			contractRequestDirection:  newContractStreamReceiver(),
			contractResponseDirection: newContractStreamReceiver(),
		},
	}

	if opened {
		stream.open()
	}

	return
}

func (stream *contractStreamState) open() {
	stream.openOnce.Do(func() {
		close(stream.opened)
	})
}

func (stream *contractStreamState) waitOpen(ctx context.Context) (err error) {
	select {
	case <-stream.opened:
	case <-ctx.Done():
		err = ctx.Err()
	case <-stream.peer.client.done:
		err = ErrClientNotConnected
	}

	return
}

func (stream *contractStreamState) canSend(direction string) (err error) {
	if direction != contractRequestDirection && direction != contractResponseDirection {
		err = fmt.Errorf("arctic: invalid contract stream direction %q", direction)
		return
	}

	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	if stream.closed[direction] {
		err = ErrContractStreamClosed
	}

	return
}

func (stream *contractStreamState) closeLocal(direction string) (err error) {
	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	if stream.closed[direction] {
		err = ErrContractStreamClosed
		return
	}

	stream.closed[direction] = true
	stream.receivers[direction].close(nil)
	return
}

func (stream *contractStreamState) remoteClose(direction string) (valid bool) {
	var receiver *contractStreamReceiver
	if receiver, valid = stream.receivers[direction]; !valid {
		return
	}

	stream.mutex.Lock()
	stream.closed[direction] = true
	stream.mutex.Unlock()
	receiver.close(stream.notifyClose)
	return
}

func (stream *contractStreamState) closeAll() {
	stream.closeOnce.Do(func() {
		stream.mutex.Lock()
		stream.closed[contractRequestDirection] = true
		stream.closed[contractResponseDirection] = true
		stream.mutex.Unlock()

		var (
			remaining         int32  = 2
			notifyWhenDrained func() = func() {
				if atomic.AddInt32(&remaining, -1) == 0 {
					stream.notifyClose()
				}
			}
		)

		stream.receivers[contractRequestDirection].close(notifyWhenDrained)
		stream.receivers[contractResponseDirection].close(notifyWhenDrained)
		stream.open()
	})
}

func (stream *contractStreamState) notifyClose() {
	stream.mutex.Lock()
	if stream.closeNotified {
		stream.mutex.Unlock()
		return
	}

	stream.closeNotified = true
	var callbacks []func() = append([]func(){}, stream.closeHooks...)
	stream.mutex.Unlock()
	for _, callback := range callbacks {
		callback()
	}
}

func (stream *contractStreamState) onClose(handler func()) {
	stream.mutex.Lock()
	stream.closeHooks = append(stream.closeHooks, handler)
	var closed bool = stream.closeNotified
	stream.mutex.Unlock()
	if closed {
		handler()
	}
}

func (stream *contractStreamState) setHandler(direction string, handler func([]byte)) {
	stream.receivers[direction].setHandler(handler)
}

func (stream *contractStreamState) deliver(direction string, payload []byte) (delivered bool) {
	if receiver, exists := stream.receivers[direction]; exists {
		delivered = receiver.deliver(payload)
	}

	return
}

func (stream *contractStreamState) begin() (err error) {
	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	if stream.started {
		err = ErrContractStreamStarted
		return
	}

	stream.started = true
	return
}

func newContractStreamReceiver() (receiver *contractStreamReceiver) {
	receiver = &contractStreamReceiver{}
	receiver.ready = sync.NewCond(&receiver.mutex)
	return
}

func (receiver *contractStreamReceiver) setHandler(handler func([]byte)) {
	receiver.mutex.Lock()
	receiver.handler = handler
	if !receiver.started {
		receiver.started = true
		go receiver.run()
	}

	receiver.ready.Signal()
	receiver.mutex.Unlock()
}

func (receiver *contractStreamReceiver) deliver(payload []byte) (delivered bool) {
	receiver.mutex.Lock()
	defer receiver.mutex.Unlock()
	if receiver.closed {
		return
	}

	if len(receiver.queue) >= contractStreamQueueLimit {
		return
	}

	receiver.queue = append(receiver.queue, payload)
	receiver.ready.Signal()
	delivered = true
	return
}

func (receiver *contractStreamReceiver) close(callback func()) {
	receiver.mutex.Lock()
	receiver.closed = true
	var callbacks []func()

	if receiver.finished {
		if callback != nil {
			callbacks = append(callbacks, callback)
		}
	} else {
		if callback != nil {
			receiver.onClose = append(receiver.onClose, callback)
		}

		if !receiver.started && len(receiver.queue) == 0 {
			receiver.finished = true
			callbacks = append(callbacks, receiver.onClose...)
			receiver.onClose = nil
		}
	}

	receiver.ready.Broadcast()
	receiver.mutex.Unlock()
	for _, closeHandler := range callbacks {
		closeHandler()
	}
}

func (receiver *contractStreamReceiver) run() {
	for {
		receiver.mutex.Lock()
		for len(receiver.queue) == 0 && !receiver.closed {
			receiver.ready.Wait()
		}

		if len(receiver.queue) == 0 && receiver.closed {
			receiver.finished = true
			var callbacks []func() = append([]func(){}, receiver.onClose...)
			receiver.onClose = nil
			receiver.mutex.Unlock()
			for _, callback := range callbacks {
				callback()
			}

			return
		}

		var (
			payload []byte       = receiver.queue[0]
			handler func([]byte) = receiver.handler
		)

		receiver.queue = receiver.queue[1:]
		receiver.mutex.Unlock()

		if handler != nil {
			handler(payload)
		}
	}
}
