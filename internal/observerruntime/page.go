// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reactors"
)

// PageEvent is one decoded event of a page. Content is *E, as DecodeContent returns.
type PageEvent struct {
	Content any
	Context events.Context
}

// PagePartition is one partition of a decoded page. Events holds the events that
// decoded, in sequence order. Failed, when set, fails the partition at FailedAt:
// the events before it may still be handled, nothing from it on is.
type PagePartition struct {
	Partition string
	Events    []PageEvent
	Failed    error
	FailedAt  events.SequenceNumber
}

// PartitionOutcome is the settled result for one partition of a page.
type PartitionOutcome struct {
	Partition      string
	State          contracts.ObservationState
	LastSuccessful events.SequenceNumber
	Err            error
	RetryAfter     time.Duration
	ReadModelState string
}

// problem is the earliest-wins input to settlement: something stopped a partition
// at a sequence number.
type problem struct {
	partition  string
	at         events.SequenceNumber
	state      contracts.ObservationState
	err        error
	retryAfter time.Duration
}

// settle reports the earliest problem of a partition, or success at its last
// event. Within a partition the earliest problem wins: everything from it on is
// delivered again, and the events before it count as handled.
func settle(partition PagePartition, problems []problem) PartitionOutcome {
	var earliest *problem
	for i := range problems {
		if problems[i].partition == partition.Partition && (earliest == nil || problems[i].at < earliest.at) {
			earliest = &problems[i]
		}
	}
	last := events.Unavailable
	for _, event := range partition.Events {
		if earliest == nil || event.Context.SequenceNumber < earliest.at {
			last = event.Context.SequenceNumber
		}
	}
	if earliest == nil {
		return PartitionOutcome{Partition: partition.Partition, State: contracts.ObservationState_Success, LastSuccessful: last}
	}
	return PartitionOutcome{Partition: partition.Partition, State: earliest.state, LastSuccessful: last, Err: earliest.err, RetryAfter: earliest.retryAfter}
}

// failAll fails every partition of a page from its start, for an activation or
// cleanup failure that leaves no partition's handling trustworthy.
func failAll(partitions []PagePartition, err error) []PartitionOutcome {
	outcomes := make([]PartitionOutcome, len(partitions))
	for i, partition := range partitions {
		outcomes[i] = PartitionOutcome{Partition: partition.Partition, State: contracts.ObservationState_Failed, LastSuccessful: events.Unavailable, Err: err}
	}
	return outcomes
}

// DispatchReactorPage hands a decoded page to a reactor and settles each
// partition on its own. Batch methods run first, once each with every event of
// the types they observe across partitions, in sequence order. Per-event handlers
// then run partition by partition, stopping a partition at its first failure.
// Each event type reaches exactly one of them, so order is kept per method.
// Without PerEvent one scope serves the whole page, matching C#.
func DispatchReactorPage(ctx context.Context, plan *reactors.Plan, partitions []PagePartition, batchContext observation.BatchContext, runtime reactors.Runtime) (outcomes []PartitionOutcome) {
	var problems []problem
	for _, partition := range partitions {
		if partition.Failed != nil {
			problems = append(problems, problem{partition.Partition, partition.FailedAt, contracts.ObservationState_Failed, partition.Failed, 0})
		}
	}
	var lease *reactors.Lease
	defer func() {
		if recovered := recover(); recovered != nil {
			outcomes = failAll(partitions, &diagnostics.PanicError{})
		}
		if lease != nil {
			if err := closeLease(ctx, lease); err != nil {
				outcomes = failAll(partitions, err)
			}
		}
		for _, outcome := range outcomes {
			if outcome.State == contracts.ObservationState_Failed {
				diagnostics.Log(ctx, plan.Logger(), slog.LevelError, "reactor partition failed", "reactor", "observe", outcome.Err)
			}
		}
	}()
	if !plan.PerEvent() {
		var err error
		if lease, err = plan.Activate(ctx); err != nil {
			return failAll(partitions, err)
		}
	}
	problems = append(problems, runBatchMethods(ctx, plan, lease, partitions, batchContext)...)
	for _, partition := range partitions {
		if p, ok := runPerEvent(ctx, plan, lease, partition, runtime); !ok {
			problems = append(problems, p)
		}
	}
	outcomes = make([]PartitionOutcome, len(partitions))
	for i, partition := range partitions {
		outcomes[i] = settle(partition, problems)
	}
	return outcomes
}

type batchItem struct {
	partition string
	event     PageEvent
}

func runBatchMethods(ctx context.Context, plan *reactors.Plan, lease *reactors.Lease, partitions []PagePartition, batchContext observation.BatchContext) []problem {
	methods := plan.BatchMethods()
	if len(methods) == 0 {
		return nil
	}
	observed := make([][]batchItem, len(methods))
	for _, partition := range partitions {
		for _, event := range partition.Events {
			if method, ok := plan.BatchMethodFor(event.Context.EventType.ID); ok {
				index := slices.IndexFunc(methods, func(m reactors.BatchMethod) bool { return m.Name() == method.Name() })
				observed[index] = append(observed[index], batchItem{partition.Partition, event})
			}
		}
	}
	var problems []problem
	for i, method := range methods {
		items := observed[i]
		if len(items) == 0 {
			continue
		}
		slices.SortStableFunc(items, func(a, b batchItem) int {
			return compareSequence(a.event.Context.SequenceNumber, b.event.Context.SequenceNumber)
		})
		values := make([]observation.EventAndContext, len(items))
		for j, item := range items {
			values[j] = observation.EventAndContext{Event: eventValue(item.event.Content), Context: item.event.Context}
		}
		batch := observation.NewEventBatch(values, batchContext.CanDefer)
		if err := invokeBatch(ctx, plan, lease, method, batch, batchContext); err != nil {
			problems = append(problems, failFirstOfEach(items, err)...)
			continue
		}
		for _, outcome := range batch.Outcomes() {
			partition := items[slices.IndexFunc(items, func(item batchItem) bool { return item.event.Context.SourceID == outcome.Partition })].partition
			state := contracts.ObservationState_Failed
			if outcome.Kind == observation.OutcomeDeferred {
				state = contracts.ObservationState_Deferred
			}
			problems = append(problems, problem{partition, outcome.At, state, outcome.Err, outcome.RetryAfter})
		}
	}
	return problems
}

func invokeBatch(ctx context.Context, plan *reactors.Plan, lease *reactors.Lease, method reactors.BatchMethod, batch *observation.EventBatch, batchContext observation.BatchContext) (err error) {
	ctx = batchInvocationContext(ctx, plan.Identifier())
	if lease == nil {
		lease, err = plan.Activate(ctx)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, closeLease(ctx, lease)) }()
	}
	return lease.InvokeBatch(ctx, method, batch, batchContext)
}

func runPerEvent(ctx context.Context, plan *reactors.Plan, lease *reactors.Lease, partition PagePartition, runtime reactors.Runtime) (problem, bool) {
	for _, event := range partition.Events {
		if _, batched := plan.BatchMethodFor(event.Context.EventType.ID); batched {
			continue
		}
		fail := func(err error) (problem, bool) {
			return problem{partition.Partition, event.Context.SequenceNumber, contracts.ObservationState_Failed, err, 0}, false
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		deliveryCtx := ReactorInvocationContext(ctx, plan.Identifier(), event.Context)
		current := lease
		if current == nil {
			var err error
			if current, err = plan.Activate(deliveryCtx); err != nil {
				return fail(err)
			}
		}
		err := current.Invoke(deliveryCtx, event.Content, event.Context, runtime)
		if lease == nil {
			err = errors.Join(err, closeLease(deliveryCtx, current))
		}
		if err != nil {
			return fail(err)
		}
	}
	return problem{}, true
}

func failFirstOfEach(items []batchItem, err error) []problem {
	var problems []problem
	for _, item := range items {
		if !slices.ContainsFunc(problems, func(p problem) bool { return p.partition == item.partition }) {
			problems = append(problems, problem{item.partition, item.event.Context.SequenceNumber, contracts.ObservationState_Failed, err, 0})
		}
	}
	return problems
}

// closeLease gives cleanup a bounded cancellation-independent budget, but never
// escapes joining. Callbacks that ignore cancellation cannot be forcibly terminated.
func closeLease(ctx context.Context, lease interface{ Close(context.Context) error }) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return lease.Close(cleanup)
}

// batchInvocationContext runs a batch method as the system on behalf of no single
// event: a batch spans events with different correlations and causers.
func batchInvocationContext(ctx context.Context, id reactors.ID) context.Context {
	ctx = metadata.WithIdentity(ctx, identities.System())
	return metadata.WithCausation(ctx, metadata.Causation{Occurred: time.Now(), Type: "Client Reactor", Properties: map[string]string{"ReactorId": string(id)}})
}

// eventValue dereferences decoded *E content so a batch's type switch matches E.
func eventValue(content any) any {
	value := reflect.ValueOf(content)
	if value.Kind() == reflect.Pointer && !value.IsNil() {
		return value.Elem().Interface()
	}
	return content
}

func compareSequence(a, b events.SequenceNumber) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// NewBatchContext describes a decoded page. Deferral is accepted only when the
// kernel delivered a page (paged), since an older kernel cannot defer.
func NewBatchContext(observer string, store metadata.StoreName, namespace metadata.Namespace, sequence events.SequenceID, origin observation.Origin, partitions []PagePartition, deliverWithin time.Duration, paged bool) observation.BatchContext {
	c := observation.BatchContext{Observer: observer, Store: store, Namespace: namespace, Sequence: sequence, Origin: origin, FirstSequenceNumber: events.Unavailable, LastSequenceNumber: events.Unavailable, CanDefer: paged}
	for _, partition := range partitions {
		c.Partitions = append(c.Partitions, events.SourceID(partition.Partition))
		for _, event := range partition.Events {
			c.EventCount++
			number := event.Context.SequenceNumber
			if c.FirstSequenceNumber == events.Unavailable || number < c.FirstSequenceNumber {
				c.FirstSequenceNumber = number
			}
			if c.LastSequenceNumber == events.Unavailable || number > c.LastSequenceNumber {
				c.LastSequenceNumber = number
			}
		}
	}
	if deliverWithin > 0 {
		c.Deadline = time.Now().Add(deliverWithin)
	}
	return c
}
