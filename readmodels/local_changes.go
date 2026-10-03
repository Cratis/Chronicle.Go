// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cratis/chronicle.go/internal/faults"
)

// ReductionChanges is a store/namespace-local notification source for successful
// in-process folds, not a durable kernel change feed. The store facade owns it;
// adapters may construct its zero value, bind a generation, and publish folds.
// It is concurrency-safe and invokes no callbacks while holding its lock.
type ReductionChanges struct {
	mu         sync.Mutex
	generation context.Context
	feeds      map[*reductionFeed]struct{}
}
type reductionMessage struct {
	key   Key
	value json.RawMessage
}
type reductionFeed struct {
	model  Identifier
	queue  chan reductionMessage
	bytes  int
	limit  int
	err    error // All feed state is protected by ReductionChanges.mu.
	closed bool
}

// BindGeneration binds future local watches to a client generation. Rebinding
// terminates old feeds; already returned watches never silently switch generation.
// The context must be a client lifetime, not a short registration/request context.
func (h *ReductionChanges) BindGeneration(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.generation == ctx || ctx.Err() != nil {
		return
	}
	for feed := range h.feeds {
		h.finish(feed, ErrInterrupted)
	}
	h.generation = ctx
}

// Publish reports a successful local fold. Nil means removed. Documents are
// snapshotted; the source never waits for slow watchers. Each overloaded watcher
// terminates independently and no failure is attributed to the durable reducer.
// generation must be the exact context passed to BindGeneration; late folds
// from retired generations are ignored. Notifications do not prove that the
// kernel persisted the returned fold yet.
func (h *ReductionChanges) Publish(generation context.Context, model Identifier, key Key, value json.RawMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.generation != generation || generation.Err() != nil {
		return
	}
	for feed := range h.feeds {
		if feed.model != model {
			continue
		}
		size := len(value) + len(key)
		if size > feed.limit-feed.bytes {
			h.finish(feed, ErrOverloaded)
			continue
		}
		copy := append(json.RawMessage(nil), value...)
		select {
		case feed.queue <- reductionMessage{key, copy}:
			feed.bytes += size
		default:
			h.finish(feed, ErrOverloaded)
		}
	}
}
func (h *ReductionChanges) finish(feed *reductionFeed, err error) {
	if feed.closed {
		return
	}
	feed.err, feed.closed = err, true
	delete(h.feeds, feed)
	close(feed.queue)
}
func (h *ReductionChanges) open(ctx context.Context, model Identifier, c watchConfig) (func() (reductionMessage, error), func(), error) {
	h.mu.Lock()
	generation := h.generation
	if generation == nil || generation.Err() != nil {
		h.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: reducer generation unavailable", ErrInterrupted)
	}
	feed := &reductionFeed{model: model, queue: make(chan reductionMessage, c.capacity), limit: c.bytes}
	if h.feeds == nil {
		h.feeds = make(map[*reductionFeed]struct{})
	}
	h.feeds[feed] = struct{}{}
	h.mu.Unlock()
	finish := func(err error) { h.mu.Lock(); h.finish(feed, err); h.mu.Unlock() }
	generationDone, callerDone := make(chan struct{}), make(chan struct{})
	stopGeneration := context.AfterFunc(generation, func() { defer close(generationDone); finish(ErrInterrupted) })
	stopCaller := context.AfterFunc(ctx, func() { defer close(callerDone); finish(ctx.Err()) })
	closeFeed := func() {
		if !stopGeneration() {
			<-generationDone
		}
		if !stopCaller() {
			<-callerDone
		}
		finish(context.Canceled)
	}
	receive := func() (reductionMessage, error) {
		message, ok := <-feed.queue
		h.mu.Lock()
		defer h.mu.Unlock()
		if !ok {
			return reductionMessage{}, feed.err
		}
		feed.bytes -= len(message.key) + len(message.value)
		return message, nil
	}
	return receive, closeFeed, nil
}

// WithReductionChanges installs the store's local reducer notification source.
// It is borrowed, and must be bound to a generation before Watch. Each watch has
// two bounded queues: raw local notifications and decoded consumer deliveries.
func WithReductionChanges(source *ReductionChanges) ServiceOption {
	return func(s *Service) error {
		if source == nil || s.reductionChanges != nil {
			return invalid("one local reducer notification source required")
		}
		s.reductionChanges = source
		return nil
	}
}
func watchReductions[T any](ctx context.Context, s *Service, d Descriptor, c watchConfig, decode func(json.RawMessage) (T, error)) (*Subscription[Change[T]], error) {
	if s.reductionChanges == nil {
		return nil, fmt.Errorf("%w: local reducer notifications unavailable", faults.ErrUnsupported)
	}
	var closeFeed func() // Written and called only by the subscription worker.
	return startSubscription(ctx, c, func(ctx context.Context) (func() (Change[T], int, bool, error), error) {
		receive, cleanup, err := s.reductionChanges.open(ctx, d.Identifier(), c)
		if err != nil {
			return nil, err
		}
		closeFeed = cleanup
		ready := false
		return func() (Change[T], int, bool, error) {
			if !ready {
				ready = true
				return Change[T]{}, 0, true, nil
			}
			message, err := receive()
			if err != nil {
				return Change[T]{}, 0, false, err
			}
			change := Change[T]{Key: message.key, Type: Modified, Context: changeContext(s, d, message.key)}
			if message.value == nil {
				change.Type = Removed
				return change, len(message.key), false, nil
			}
			released, err := s.Release(ctx, d.Identifier(), message.value)
			if err != nil {
				return Change[T]{}, 0, false, err
			}
			data, err := normalizeID(released, d)
			if err == nil {
				change.Value, err = decode(data)
			}
			if err != nil {
				return Change[T]{}, 0, false, err
			}
			change.HasValue = true
			return change, len(data) + len(message.key), false, nil
		}, nil
	}, func() {
		if closeFeed != nil {
			closeFeed()
		}
	})
}
