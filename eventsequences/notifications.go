// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

// AppendEvent identifies an original input, not a durable event delivery. It
// contains no caller-owned event value or payload. Batch entries retain input order.
type AppendEvent struct {
	// Source is the attempted event source.
	Source events.SourceID
	// EventType is the registered type and generation submitted for this input.
	EventType events.TypeRef
	// Route is the normalized source/stream classification submitted to the kernel.
	Route Route
	// Position is present only for confirmed persistence; zero is a valid position.
	Position *events.SequenceNumber
}

// AppendNotification describes one local, nonempty append attempt after its
// result is known. It is not an observer subscription, replay or proof of durable
// delivery. Each callback owns its Events and Result, including nested collections
// and positions. Err and any wrapped errors are borrowed and must not be mutated.
type AppendNotification struct {
	// CorrelationID is the effective REQUEST correlation, even if the response is
	// absent or carries a different ID. Multiple executions may share this ID;
	// use Origin to distinguish them.
	CorrelationID metadata.CorrelationID
	// Origin identifies the unit of work for a commit. Immediate appends use
	// the handled resolver result (including zero), otherwise the context origin.
	// Zero means unattributed.
	Origin Origin
	// Operation identifies the store, namespace, sequence and distinct exact types.
	Operation OperationMetadata
	// Events identifies each original input and its position when known.
	Events []AppendEvent
	// Result preserves the append disposition and all diagnostics, without
	// inferring success from completion. A single append is represented as a
	// one-entry BatchResult. Its CorrelationID remains the response correlation.
	Result BatchResult
	// Err is the append operation error before notification delivery. Domain
	// rejections may have nil Err: always inspect Result.Disposition/Result.Err().
	Err error
}

// AppendCallbackPanicError reports a subscriber panic without changing the
// append disposition. Other admitted subscribers still receive the result. The
// append returns this error joined with any operation error; never retry a
// committed or unknown append because notification delivery failed.
type AppendCallbackPanicError struct {
	// Value is the recovered panic value. It is borrowed and must not be mutated.
	Value any
}

func (e *AppendCallbackPanicError) Error() string {
	return fmt.Sprintf("chronicle: append callback panicked: %v", e.Value)
}

type appendSubscriptions struct {
	mu   sync.Mutex
	list []*appendSubscription
}

type appendSubscription struct {
	callback func(AppendNotification) // Guarded by appendSubscriptions.mu.
}

// OnAppend subscribes to this Sequence. EventStore caches handles, so subscriptions
// are shared per client, store, namespace and sequence. Low-level New handles keep
// independent subscriptions. Nil callbacks return a no-op unsubscribe.
// The caller must unsubscribe to release the callback and its captured state.
// Unsubscribe is concurrency-safe, idempotent and nonblocking, including inside
// callbacks. It prevents new callback admissions but does not join an already
// admitted invocation. Join command-owned appends before disposing command state.
//
// Subscribers run synchronously in subscription order, after a validated,
// nonempty request is handed to the RPC client and its result becomes known,
// before Append/AppendMany/AppendBatch/AppendPreparedBatch returns. Staging,
// pre-dispatch failures, rollback and eventless operations do not notify.
// Unit-of-work commits notify through AppendPreparedBatch, before unit completion.
// The subscriber list is snapshotted at delivery; subscriptions added during a
// callback start with the next delivery. Disposal before admission skips it.
//
// No internal locks are held during callbacks. Callbacks may subscribe, dispose
// or append recursively (the caller must bound recursion). Concurrent appends may
// invoke the same callback concurrently, with no cross-operation ordering; protect
// shared command state and filter Origin. Slow callbacks delay the caller.
// Panics become AppendCallbackPanicError, not a changed write disposition or a
// silent failure. Notification Err excludes subscriber failures.
func (s *Sequence) OnAppend(callback func(AppendNotification)) (unsubscribe func()) {
	if callback == nil {
		return func() {}
	}
	subscription := &appendSubscription{callback: callback}
	s.appends.mu.Lock()
	s.appends.list = append(s.appends.list, subscription)
	s.appends.mu.Unlock()
	return func() {
		s.appends.mu.Lock()
		defer s.appends.mu.Unlock()
		if subscription.callback == nil {
			return
		}
		subscription.callback = nil
		index := slices.Index(s.appends.list, subscription)
		s.appends.list = slices.Delete(s.appends.list, index, index+1)
	}
}

func (s *Sequence) notifyAppend(notification AppendNotification) error {
	if len(notification.Events) == 0 {
		return nil
	}
	s.appends.mu.Lock()
	subscriptions := slices.Clone(s.appends.list)
	s.appends.mu.Unlock()
	var failures []error
	for _, subscription := range subscriptions {
		s.appends.mu.Lock()
		callback := subscription.callback
		s.appends.mu.Unlock()
		if callback != nil {
			if err := invokeAppendCallback(callback, cloneNotification(notification)); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func invokeAppendCallback(callback func(AppendNotification), notification AppendNotification) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = &AppendCallbackPanicError{Value: value}
		}
	}()
	callback(notification)
	return nil
}

func cloneNotification(notification AppendNotification) AppendNotification {
	notification.Events = slices.Clone(notification.Events)
	for i := range notification.Events {
		notification.Events[i].Position = copyPointer(notification.Events[i].Position)
	}
	result := &notification.Result
	result.Positions = slices.Clone(result.Positions)
	result.Errors = slices.Clone(result.Errors)
	result.ConcurrencyViolations = slices.Clone(result.ConcurrencyViolations)
	result.ConstraintViolations = slices.Clone(result.ConstraintViolations)
	for i := range result.ConstraintViolations {
		result.ConstraintViolations[i].Details = maps.Clone(result.ConstraintViolations[i].Details)
	}
	result.Target.First = copyPointer(result.Target.First)
	result.Target.EventTypeTails = maps.Clone(result.Target.EventTypeTails)
	return notification
}

func (s *Sequence) notifySingle(origin Origin, source events.SourceID, ref events.TypeRef, route Route, correlation metadata.CorrelationID, result AppendResult, err error) error {
	batch := BatchResult{Disposition: result.Disposition, CorrelationID: result.CorrelationID,
		ConstraintViolations: result.ConstraintViolations, ConcurrencyViolations: result.ConcurrencyViolations,
		Errors: result.Errors, ConcurrencyCheckPerformed: result.ConcurrencyCheckPerformed, Target: result.Target}
	if result.Position != nil {
		batch.Positions = []events.SequenceNumber{*result.Position}
	}
	return s.notifyAppend(AppendNotification{
		Origin: origin, CorrelationID: correlation, Operation: OperationMetadata{store: s.store, namespace: s.namespace, sequence: s.id, refs: []events.TypeRef{ref}},
		Events: []AppendEvent{{Source: source, EventType: ref, Route: normalizedRoute(route), Position: result.Position}}, Result: batch, Err: err,
	})
}

func (s *Sequence) notifyBatch(origin Origin, batch preparedBatch, result BatchResult, err error) error {
	notification := AppendNotification{Origin: origin, CorrelationID: wire.Correlation(batch.request.CorrelationId), Result: result, Err: err,
		Operation: OperationMetadata{store: s.store, namespace: s.namespace, sequence: s.id}}
	seen := make(map[events.TypeRef]bool)
	for i, event := range batch.request.Events {
		input := AppendEvent{Source: events.SourceID(event.EventSourceId), EventType: batch.refs[i],
			Route: Route{SourceType: events.SourceType(event.EventSourceType), StreamType: events.StreamType(event.EventStreamType), StreamID: events.StreamID(event.EventStreamId)}}
		if result.Disposition == Committed && i < len(result.Positions) {
			input.Position = &result.Positions[i]
		}
		notification.Events = append(notification.Events, input)
		if !seen[input.EventType] {
			seen[input.EventType] = true
			notification.Operation.refs = append(notification.Operation.refs, input.EventType)
		}
	}
	return s.notifyAppend(notification)
}

func joinNotificationError(operation, delivery error) error {
	if delivery == nil {
		return operation
	}
	return errors.Join(operation, delivery)
}
