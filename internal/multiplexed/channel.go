// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package multiplexed carries every reactor and reducer observation of one
// connection over a single bidirectional call to the kernel's
// MultiplexedObservation service.
//
// A Channel owns the shared call: the first stream opens it, the last stream
// closing cancels it, and a call that failed is replaced by a new one for the
// next stream. Each stream has its own ordered, unbounded receive queue, so a
// slow observer never blocks the reader that serves the others.
package multiplexed

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation/multiplexed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Capability is the name the kernel advertises in its compatibility response
// when it serves the MultiplexedObservation service.
const Capability = "multiplexed-observation"

const (
	// DefaultCoalesce is how long the writer waits for companions of the first
	// pending message before it writes.
	DefaultCoalesce = 10 * time.Millisecond
	// MaxMessagesPerWrite bounds the stream messages carried by one write.
	MaxMessagesPerWrite = 500
)

// Call is the shared bidirectional call.
type Call = grpc.BidiStreamingClient[contracts.MultiplexedClientMessage, contracts.MultiplexedServerMessage]

// Opener opens the shared call on a context that lives as long as the call.
type Opener func(ctx context.Context) (Call, error)

// Channel multiplexes observer streams over one call at a time.
type Channel struct {
	base     context.Context
	open     Opener
	coalesce time.Duration

	mu     sync.Mutex
	call   *call
	nextID uint64
}

// NewChannel returns a channel whose calls end with base. coalesce is the
// writer's wait for companion messages; zero or negative writes at once.
func NewChannel(base context.Context, open Opener, coalesce time.Duration) *Channel {
	return &Channel{base: base, open: open, coalesce: coalesce}
}

// Open starts a stream of the given kind, opening the shared call when none is
// alive. ctx bounds the stream: when it ends the stream is closed.
func (c *Channel) Open(ctx context.Context, kind contracts.MultiplexedObserverKind) (*Stream, error) {
	for {
		c.mu.Lock()
		current := c.call
		if current == nil || current.hasEnded() {
			opened, err := c.openCall()
			if err != nil {
				c.mu.Unlock()
				return nil, err
			}
			current, c.call = opened, opened
		}
		c.nextID++
		id := c.nextID
		c.mu.Unlock()
		if stream := current.add(ctx, id, kind); stream != nil {
			return stream, nil
		}
		// The call ended between the check and the add: start another.
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
	}
}

func (c *Channel) openCall() (*call, error) {
	ctx, cancel := context.WithCancel(c.base)
	stream, err := c.open(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	shared := &call{stream: stream, cancel: cancel, coalesce: c.coalesce, streams: map[uint64]*Stream{}, wake: make(chan struct{}, 1), ctx: ctx, ended: make(chan struct{})}
	shared.workers.Add(2)
	go shared.read()
	go shared.write()
	return shared, nil
}

type call struct {
	stream   Call
	ctx      context.Context
	cancel   context.CancelFunc
	coalesce time.Duration
	workers  sync.WaitGroup

	mu      sync.Mutex
	streams map[uint64]*Stream
	outbox  []*contracts.MultiplexedStreamMessage
	wake    chan struct{}
	err     error
	ended   chan struct{}
}

func (c *call) hasEnded() bool {
	select {
	case <-c.ended:
		return true
	default:
		return false
	}
}

func (c *call) add(ctx context.Context, id uint64, kind contracts.MultiplexedObserverKind) *Stream {
	s := &Stream{call: c, id: id, kind: kind, ctx: ctx, wake: make(chan struct{}, 1)}
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return nil
	}
	c.streams[id] = s
	c.mu.Unlock()
	// A stream whose context ends closes itself, as a cancelled per-observer call would.
	stop := context.AfterFunc(ctx, s.Close)
	s.mu.Lock()
	s.stop = stop
	s.mu.Unlock()
	return s
}

// enqueue queues a stream message for the writer. It fails when the call ended.
func (c *call) enqueue(message *contracts.MultiplexedStreamMessage) error {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return io.EOF
	}
	c.outbox = append(c.outbox, message)
	c.mu.Unlock()
	c.signal()
	return nil
}

func (c *call) signal() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// release removes a finished stream; the last one ends the call.
func (c *call) release(id uint64) {
	c.mu.Lock()
	delete(c.streams, id)
	last := len(c.streams) == 0 && c.err == nil
	c.mu.Unlock()
	if last {
		c.end(errClosed)
	}
}

var errClosed = errors.New("multiplexed: the last stream closed")

// end records why the call is over, ends every open stream with it and cancels
// the shared call. The first reason wins.
func (c *call) end(reason error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = reason
	open := make([]*Stream, 0, len(c.streams))
	for _, s := range c.streams {
		open = append(open, s)
	}
	c.streams = nil
	c.outbox = nil
	c.mu.Unlock()
	close(c.ended)
	c.cancel()
	for _, s := range open {
		s.finish(reason)
	}
}

func (c *call) read() {
	defer c.workers.Done()
	for {
		message, err := c.stream.Recv()
		if err != nil {
			// A clean end of the call completes every stream.
			c.end(err)
			return
		}
		if err = c.deliver(message); err != nil {
			c.end(err)
			return
		}
	}
}

// deliver hands one server message to its stream. A sequence that is not the
// next one means a message was lost or reordered, which fails the whole call.
func (c *call) deliver(message *contracts.MultiplexedServerMessage) error {
	if message == nil {
		return status.Error(codes.Internal, "multiplexed: empty server message")
	}
	c.mu.Lock()
	s := c.streams[message.StreamId]
	c.mu.Unlock()
	if s == nil {
		// Messages in flight for a stream this client already closed.
		return nil
	}
	if message.Sequence != s.received+1 {
		return status.Errorf(codes.Internal, "multiplexed: stream %d expected sequence %d but received %d", message.StreamId, s.received+1, message.Sequence)
	}
	s.received = message.Sequence
	if end := message.End; end != nil {
		if end.StatusCode == 0 {
			s.finish(io.EOF)
		} else {
			s.finish(status.Error(codes.Code(end.StatusCode), end.Detail))
		}
		c.release(s.id)
		return nil
	}
	s.push(message)
	return nil
}

func (c *call) write() {
	defer c.workers.Done()
	for {
		select {
		case <-c.wake:
		case <-c.ctx.Done():
			return
		}
		if c.coalesce > 0 && c.pending() < MaxMessagesPerWrite {
			timer := time.NewTimer(c.coalesce)
			select {
			case <-timer.C:
			case <-c.ctx.Done():
				timer.Stop()
				return
			}
		}
		batch := c.take()
		if len(batch) == 0 {
			continue
		}
		if err := c.stream.Send(&contracts.MultiplexedClientMessage{Messages: batch}); err != nil {
			if !errors.Is(err, io.EOF) {
				c.end(err)
			}
			// On io.EOF the call is over and the reader reports why.
			return
		}
	}
}

func (c *call) pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.outbox)
}

func (c *call) take() []*contracts.MultiplexedStreamMessage {
	c.mu.Lock()
	n := min(len(c.outbox), MaxMessagesPerWrite)
	batch := append([]*contracts.MultiplexedStreamMessage(nil), c.outbox[:n]...)
	c.outbox = c.outbox[n:]
	more := len(c.outbox) > 0
	c.mu.Unlock()
	if more {
		c.signal()
	}
	return batch
}
