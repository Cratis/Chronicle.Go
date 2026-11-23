// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package multiplexed

import (
	"context"
	"io"
	"sync"

	contracts "github.com/cratis/chronicle.go/contracts/observation/multiplexed"
	"google.golang.org/grpc/status"
)

// Stream is one observer's side of the shared call. Send is for one goroutine
// and Receive for another, as for any gRPC stream.
type Stream struct {
	call *call
	id   uint64
	kind contracts.MultiplexedObserverKind
	ctx  context.Context
	stop func() bool

	sendMu sync.Mutex
	sent   bool

	// received is the last sequence delivered; only the call's reader uses it.
	received uint64

	mu       sync.Mutex
	queue    []*contracts.MultiplexedServerMessage
	terminal error
	closed   bool
	wake     chan struct{}
}

// Send queues a registration or a result for the kernel. The first message of a
// stream carries the observer kind. It returns io.EOF when the stream or the
// shared call has ended; Receive then reports why.
func (s *Stream) Send(message *contracts.MultiplexedStreamMessage) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if s.isOver() {
		return io.EOF
	}
	message.StreamId = s.id
	if !s.sent {
		message.Kind = s.kind
	}
	if err := s.call.enqueue(message); err != nil {
		return err
	}
	s.sent = true
	return nil
}

// Receive returns the next server message of this stream in order. A clean end
// is io.EOF; a failure is the error that ended the stream or the shared call.
// Messages that arrived before the end are delivered first.
func (s *Stream) Receive() (*contracts.MultiplexedServerMessage, error) {
	for {
		s.mu.Lock()
		if len(s.queue) > 0 {
			message := s.queue[0]
			s.queue[0] = nil
			s.queue = s.queue[1:]
			s.mu.Unlock()
			return message, nil
		}
		terminal := s.terminal
		s.mu.Unlock()
		if terminal != nil {
			return nil, terminal
		}
		select {
		case <-s.wake:
		case <-s.ctx.Done():
			return nil, status.FromContextError(s.ctx.Err()).Err()
		}
	}
}

// Close ends the stream: the kernel is told to close it and it leaves the
// shared call, which ends when no stream is left. It is safe to call twice.
func (s *Stream) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	kernelKnows := s.terminal == nil
	stop := s.stop
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	s.sendMu.Lock()
	// A stream the kernel or a failure already ended needs no Close.
	if s.sent && kernelKnows {
		_ = s.call.enqueue(&contracts.MultiplexedStreamMessage{StreamId: s.id, Close: true})
	}
	s.sendMu.Unlock()
	reason := error(io.EOF)
	if err := s.ctx.Err(); err != nil {
		reason = status.FromContextError(err).Err()
	}
	s.finish(reason)
	s.call.release(s.id)
}

func (s *Stream) isOver() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed || s.terminal != nil
}

func (s *Stream) push(message *contracts.MultiplexedServerMessage) {
	s.mu.Lock()
	if s.terminal != nil {
		s.mu.Unlock()
		return
	}
	s.queue = append(s.queue, message)
	s.mu.Unlock()
	s.notify()
}

// finish ends the stream with err once the messages already queued are read.
func (s *Stream) finish(err error) {
	s.mu.Lock()
	if s.terminal == nil {
		s.terminal = err
	}
	s.mu.Unlock()
	s.notify()
}

func (s *Stream) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
