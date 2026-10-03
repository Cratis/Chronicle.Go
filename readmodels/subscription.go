// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"

	"github.com/cratis/chronicle.go/internal/wire"
)

// ErrInterrupted means a best-effort feed ended. There is no durable cursor:
// re-watch and re-fetch state; neither operation promises gap-free recovery.
var ErrInterrupted = errors.New("chronicle: read-model subscription interrupted")

// ErrOverloaded means the subscription's bounded queue filled. No changes are
// silently dropped: the stream terminates and requires a new watch and snapshot.
var ErrOverloaded = errors.New("chronicle: read-model subscription overloaded")

// WatchOption configures a subscription. Scalar options are last-wins.
type WatchOption func(*watchConfig)
type watchConfig struct{ capacity, bytes int }

// WithWatchBuffer bounds queued messages and their serialized bytes (defaults:
// 64 messages, 8 MiB). Both must be positive. A single oversized message also
// terminates with ErrOverloaded. Decoded Go values and gRPC's own buffers are extra.
func WithWatchBuffer(messages, bytes int) WatchOption {
	return func(c *watchConfig) { c.capacity, c.bytes = messages, bytes }
}

// ValidateWatchOptions validates options without opening a stream. Registration
// adapters use it to reject invalid queue configuration before client startup.
func ValidateWatchOptions(options ...WatchOption) error { _, err := watchOptions(options); return err }

func watchOptions(options []WatchOption) (watchConfig, error) {
	c := watchConfig{64, 8 << 20}
	for _, option := range options {
		if option == nil {
			return c, invalid("nil watch option")
		}
		option(&c)
	}
	if c.capacity < 1 || c.bytes < 1 {
		return c, invalid("positive watch buffer limits required")
	}
	return c, nil
}

type queued[T any] struct {
	value T
	bytes int
}

// Subscription owns one best-effort stream and its receive worker. One consumer
// may call Recv or iterate Values. Close is concurrency-safe and joins the worker;
// always close after early consumption. Buffered items precede terminal errors.
// Its zero value is invalid. It never reconnects or implies a current snapshot.
type Subscription[T any] struct {
	cancel context.CancelFunc
	done   chan struct{}
	queue  chan queued[T]
	mu     sync.Mutex
	bytes  int
	err    error
}

// Recv returns the next owned value, or the retained terminal error. Cancellation
// is controlled by the context supplied when opening the subscription.
func (s *Subscription[T]) Recv() (T, error) {
	item, ok := <-s.queue
	if ok {
		s.mu.Lock()
		s.bytes -= item.bytes
		s.mu.Unlock()
		return item.value, nil
	}
	var zero T
	return zero, s.Err()
}

// Done closes when the receive worker has exited (buffered items may remain).
func (s *Subscription[T]) Done() <-chan struct{} { return s.done }

// Err returns nil while running and the retained error after termination.
func (s *Subscription[T]) Err() error { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

// Close cancels and joins the receive worker. Repeated calls are safe. It returns
// nil for successful cleanup; inspect Err or Recv for the terminal stream outcome.
func (s *Subscription[T]) Close() error { s.cancel(); <-s.done; return nil }

// Values iterates in receive order, yields the terminal error once, and closes
// on exhaustion or early break. Like Recv, it permits only one consumer.
func (s *Subscription[T]) Values() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		defer func() { _ = s.Close() }() // Close only cancels and joins; it cannot fail.
		for {
			value, err := s.Recv()
			if !yield(value, err) || err != nil {
				return
			}
		}
	}
}

// startSubscription owns open as well as receive, allowing cancellation to join
// startup failure. Ready is signaled by a protocol marker or first full window.
func startSubscription[T any](ctx context.Context, c watchConfig, open func(context.Context) (func() (T, int, bool, error), error), cleanup ...func()) (*Subscription[T], error) {
	ctx, cancel := context.WithCancel(ctx)
	s := &Subscription[T]{cancel: cancel, done: make(chan struct{}), queue: make(chan queued[T], c.capacity)}
	ready := make(chan struct{})
	go func() {
		var terminal error
		defer func() {
			cancel()
			for _, closeResource := range cleanup {
				closeResource()
			}
			s.mu.Lock()
			s.err = terminal
			s.mu.Unlock()
			close(s.queue)
			close(s.done)
		}()
		receive, err := open(ctx)
		if err != nil {
			terminal = streamError(ctx, err)
			return
		}
		subscribed := false
		for {
			value, size, marker, err := receive()
			if err != nil {
				terminal = streamError(ctx, err)
				return
			}
			if ctx.Err() != nil {
				terminal = ctx.Err()
				return
			}
			if marker {
				if subscribed {
					terminal = protocol("duplicate subscribed signal")
					return
				}
				subscribed = true
				close(ready)
				continue
			}
			// The kernel attaches its forwarder before sending Subscribed. Keep
			// racing changes bounded and ordered, but do not publish readiness yet.
			s.mu.Lock()
			if size > c.bytes-s.bytes {
				s.mu.Unlock()
				terminal = ErrOverloaded
				return
			}
			s.bytes += size
			select {
			case s.queue <- queued[T]{value, size}:
				s.mu.Unlock()
			default:
				s.bytes -= size
				s.mu.Unlock()
				terminal = ErrOverloaded
				return
			}
		}
	}()
	select {
	case <-ready:
		return s, nil
	case <-s.done:
		// A ready signal followed immediately by interruption still owns a
		// readable prefix. Do not nondeterministically discard it when both
		// channels became ready before the opening goroutine was scheduled.
		select {
		case <-ready:
			return s, nil
		default:
			return nil, s.Err()
		}
	case <-ctx.Done():
		<-s.done
		select {
		case <-ready:
			return s, nil
		default:
			return nil, s.Err()
		}
	}
}

func streamError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, ErrOverloaded) {
		return err
	}
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: %w", ErrInterrupted, io.EOF)
	}
	return errors.Join(ErrInterrupted, wire.RPCError(err))
}
