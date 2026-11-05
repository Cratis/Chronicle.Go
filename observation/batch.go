// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observation

import (
	"fmt"
	"iter"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
)

// Origin says which delivery path a page of events came from, matching C#
// ObservationOrigin. The zero value is OriginUnknown, used when a kernel does not
// report an origin and for local folds such as passive reducer reads.
type Origin int

const (
	// OriginUnknown means the kernel did not say where the page came from.
	OriginUnknown Origin = iota
	// OriginLive means the events were appended now and delivered to an observer
	// that is up to date.
	OriginLive
	// OriginCatchUp means the events were read from storage for an observer that is behind.
	OriginCatchUp
	// OriginReplay means the events were read from storage for an observer that was rewound.
	OriginReplay
	// OriginRetry means the events were read from storage to recover a failed or
	// deferred partition.
	OriginRetry
)

// String returns the C# enum member name.
func (o Origin) String() string {
	switch o {
	case OriginLive:
		return "Live"
	case OriginCatchUp:
		return "CatchUp"
	case OriginReplay:
		return "Replay"
	case OriginRetry:
		return "Retry"
	default:
		return "Unknown"
	}
}

// Observed names one event type a batch method observes. It is Go's form of the
// C# [Observes<TEvent>] attribute: create it with Observes and pass one per event
// type to a reactor or reducer batch registration. The zero value is invalid.
type Observed struct{ typ reflect.Type }

// Observes declares that a batch method observes E. E is a registered event type
// or an interface every observed event type implements; an interface selects each
// registered event type that implements it.
func Observes[E any]() Observed { return Observed{typ: reflect.TypeFor[E]()} }

// Type returns the observed Go type, nil for the zero value.
func (o Observed) Type() reflect.Type { return o.typ }

// EventAndContext is one event of a batch with its delivery context. Event holds
// the decoded event value E (not *E), so a type switch matches `case E:`.
type EventAndContext struct {
	Event   any
	Context events.Context
}

// BatchContext describes the page a batch method is called for. It is read-only
// information: changing a copy has no effect on delivery. Per-event methods
// cannot take it; registering one that does fails.
type BatchContext struct {
	// Observer is the reactor or reducer identity.
	Observer string
	// Store and Namespace identify where the events were appended.
	Store     metadata.StoreName
	Namespace metadata.Namespace
	// Sequence is the observed event sequence.
	Sequence events.SequenceID
	// Origin says which delivery path the page came from.
	Origin Origin
	// Partitions lists the page's partitions (event source IDs) in delivery order.
	// The slice is owned by the context; do not modify it.
	Partitions []events.SourceID
	// EventCount is the number of events in the page, across all its partitions.
	EventCount int
	// FirstSequenceNumber and LastSequenceNumber bound the page; both are
	// events.Unavailable for an empty page.
	FirstSequenceNumber events.SequenceNumber
	LastSequenceNumber  events.SequenceNumber
	// Deadline is when the kernel stops waiting for the result, zero when it set none.
	// Cancellation itself arrives through the method's context.Context.
	Deadline time.Time
	// CanDefer reports whether the kernel accepts deferred partitions. It is false
	// for a kernel that does not deliver pages, where a deferral fails the
	// partition with ErrDeferralNotSupportedByKernel instead.
	CanDefer bool
}

// IsReplay reports whether the page is part of a replay.
func (c BatchContext) IsReplay() bool { return c.Origin == OriginReplay }

// OutcomeKind distinguishes a failed partition from a deferred one.
type OutcomeKind int

const (
	// OutcomeFailed fails the partition from the reported event.
	OutcomeFailed OutcomeKind = iota
	// OutcomeDeferred delivers the partition again from the reported event after a delay.
	OutcomeDeferred
)

// BatchOutcome is one Fail or Defer reported on a batch.
type BatchOutcome struct {
	// Partition is the event source ID the outcome applies to.
	Partition events.SourceID
	// At is the sequence number of the first event the outcome applies to.
	At events.SequenceNumber
	// Kind is OutcomeFailed or OutcomeDeferred.
	Kind OutcomeKind
	// Err explains a failure; nil for a deferral.
	Err error
	// RetryAfter is the delay before a deferred partition is delivered again.
	RetryAfter time.Duration
}

// EventBatch holds the events a batch method receives, in sequence order, and the
// outcomes it reports for them. Returning nil from the batch method means every
// partition in the batch succeeded; returning an error fails every partition at
// its first event in the batch. Fail and Defer report a single partition instead.
// Its methods are safe for concurrent use, so outcomes may be reported from
// goroutines the batch method joins before it returns.
type EventBatch struct {
	events   []EventAndContext
	canDefer bool
	mu       sync.Mutex
	outcomes []BatchOutcome
}

// NewEventBatch creates a batch over items, which must be in sequence order.
// The client creates batches for delivery; use this to call a batch method in a
// test. canDefer selects whether Defer is honored or turned into a failure, as for
// a kernel that delivers pages or one that does not. The slice is copied.
func NewEventBatch(items []EventAndContext, canDefer bool) *EventBatch {
	return &EventBatch{events: slices.Clone(items), canDefer: canDefer}
}

// Len returns the number of events in the batch.
func (b *EventBatch) Len() int { return len(b.events) }

// At returns the event at index i, in sequence order. It panics when i is out of range.
func (b *EventBatch) At(i int) EventAndContext { return b.events[i] }

// All iterates the events in sequence order with their index.
func (b *EventBatch) All() iter.Seq2[int, EventAndContext] {
	return func(yield func(int, EventAndContext) bool) {
		for i, item := range b.events {
			if !yield(i, item) {
				return
			}
		}
	}
}

// Events returns a copy of the events in sequence order.
func (b *EventBatch) Events() []EventAndContext { return slices.Clone(b.events) }

// Partitions returns the distinct partitions (event source IDs) in the batch, in
// the order their first event appears.
func (b *EventBatch) Partitions() []events.SourceID {
	var partitions []events.SourceID
	for _, item := range b.events {
		if !slices.Contains(partitions, item.Context.SourceID) {
			partitions = append(partitions, item.Context.SourceID)
		}
	}
	return partitions
}

// Fail fails partition from its first event in the batch. Its other partitions
// still succeed. A nil err is reported as ErrPartitionFailedInBatch. It returns
// ErrPartitionNotInBatch when the batch holds no events for partition.
func (b *EventBatch) Fail(partition events.SourceID, err error) error {
	at, ok := b.first(partition)
	if !ok {
		return notInBatch(partition)
	}
	return b.report(partition, at, OutcomeFailed, err, 0)
}

// FailEvent fails the partition of the event from that event on. The partition's
// earlier events count as handled. It returns ErrPartitionNotInBatch when the
// batch holds no event with that source ID and sequence number.
func (b *EventBatch) FailEvent(event events.Context, err error) error {
	if !b.holds(event) {
		return notInBatch(event.SourceID)
	}
	return b.report(event.SourceID, event.SequenceNumber, OutcomeFailed, err, 0)
}

// Defer asks for partition to be delivered again from its first event in the batch
// after retryAfter. A deferral is not a failure and does not count towards
// quarantine. Negative delays are treated as zero. On a kernel that does not
// deliver pages the partition fails with ErrDeferralNotSupportedByKernel instead.
func (b *EventBatch) Defer(partition events.SourceID, retryAfter time.Duration) error {
	at, ok := b.first(partition)
	if !ok {
		return notInBatch(partition)
	}
	return b.report(partition, at, OutcomeDeferred, nil, retryAfter)
}

// DeferEvent defers the partition of the event from that event on; see Defer.
func (b *EventBatch) DeferEvent(event events.Context, retryAfter time.Duration) error {
	if !b.holds(event) {
		return notInBatch(event.SourceID)
	}
	return b.report(event.SourceID, event.SequenceNumber, OutcomeDeferred, nil, retryAfter)
}

// Outcomes returns a copy of the outcomes reported so far, in reporting order.
func (b *EventBatch) Outcomes() []BatchOutcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	return slices.Clone(b.outcomes)
}

func (b *EventBatch) first(partition events.SourceID) (events.SequenceNumber, bool) {
	for _, item := range b.events {
		if item.Context.SourceID == partition {
			return item.Context.SequenceNumber, true
		}
	}
	return events.Unavailable, false
}

func (b *EventBatch) holds(event events.Context) bool {
	return slices.ContainsFunc(b.events, func(item EventAndContext) bool {
		return item.Context.SourceID == event.SourceID && item.Context.SequenceNumber == event.SequenceNumber
	})
}

func (b *EventBatch) report(partition events.SourceID, at events.SequenceNumber, kind OutcomeKind, err error, retryAfter time.Duration) error {
	retryAfter = max(retryAfter, 0)
	if kind == OutcomeFailed && err == nil {
		err = fmt.Errorf("%w: partition %q", ErrPartitionFailedInBatch, partition)
	}
	if kind == OutcomeDeferred && !b.canDefer {
		kind, retryAfter = OutcomeFailed, 0
		err = fmt.Errorf("%w: partition %q was deferred; it is failed instead", ErrDeferralNotSupportedByKernel, partition)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.outcomes = append(b.outcomes, BatchOutcome{Partition: partition, At: at, Kind: kind, Err: err, RetryAfter: retryAfter})
	return nil
}

func notInBatch(partition events.SourceID) error {
	return fmt.Errorf("%w: %q", ErrPartitionNotInBatch, partition)
}
