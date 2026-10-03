// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package observerruntime owns ordered observer delivery and acknowledgement.
package observerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reactors"
	"google.golang.org/grpc"
)

// Reactor owns a single duplex stream. Run has one receiver and one sender with
// no application queue: transport backpressure bounds pending messages.
type Reactor struct {
	plan      *reactors.Plan
	stream    grpc.BidiStreamingClient[contracts.ReactorMessage, contracts.EventsToObserve]
	runtime   reactors.Runtime
	store     metadata.StoreName
	namespace metadata.Namespace
}

// Open sends the registration on an explicit generation lifetime. The protocol
// has no registration acknowledgement: success means sent, not kernel acceptance.
func Open(ctx context.Context, conn grpc.ClientConnInterface, connectionID string, store metadata.StoreName, namespace metadata.Namespace, plan *reactors.Plan, runtime reactors.Runtime) (*Reactor, error) {
	stream, err := contracts.NewReactorsClient(conn).Observe(ctx)
	if err != nil {
		return nil, err
	}
	definition := &contracts.ReactorDefinition{
		ReactorId: string(plan.Identifier()), EventSequenceId: string(plan.EventSequence()),
		IsReplayable: plan.IsReplayable(), Tags: plan.Tags(),
		Filters: &contracts.ObserverFilters{FilterTags: plan.FilterTags(), EventSourceType: string(plan.EventSourceType()), EventStreamType: string(plan.EventStreamType())},
	}
	for _, ref := range plan.EventTypes() {
		definition.EventTypes = append(definition.EventTypes, &contracts.EventTypeWithKeyExpression{EventType: &contracts.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)}, Key: "$eventSourceId"})
	}
	err = stream.Send(&contracts.ReactorMessage{Content: &contracts.OneOf_RegisterReactor_ReactorResult{Value0: &contracts.RegisterReactor{ConnectionId: connectionID, EventStore: string(store), Namespace: string(namespace), Reactor: definition}}})
	if err != nil {
		return nil, err
	}
	return &Reactor{plan: plan, stream: stream, runtime: runtime, store: store, namespace: namespace}, nil
}

// Run processes batches sequentially until cancellation or stream failure. Effects
// are never retried here. The kernel controls failure recovery and resume position.
func (r *Reactor) Run(ctx context.Context) error {
	return runStream(ctx, r.stream.Recv, func(ctx context.Context, batch *contracts.EventsToObserve) *contracts.ReactorResult {
		if batch.ReplayState != contracts.ReplayState_REPLAY_STATE_None {
			return nil
		}
		return r.handle(ctx, batch)
	}, func(result *contracts.ReactorResult) error {
		return r.stream.Send(&contracts.ReactorMessage{Content: &contracts.OneOf_RegisterReactor_ReactorResult{Value1: result}})
	})
}
func (r *Reactor) handle(ctx context.Context, batch *contracts.EventsToObserve) (result *contracts.ReactorResult) {
	result = &contracts.ReactorResult{Partition: batch.Partition, State: contracts.ObservationState_Success, LastSuccessfulObservation: uint64(events.Unavailable)}
	var failure error
	var lease *reactors.Lease
	defer func() {
		if recovered := recover(); recovered != nil {
			failure = fmt.Errorf("reactor batch panic: %v", recovered)
		}
		if lease != nil {
			// Cleanup gets a bounded cancellation-independent budget, but never escapes
			// joining. Callbacks that ignore cancellation cannot be forcibly terminated.
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err := lease.Close(cleanup)
			cancel()
			if err != nil {
				failure = errors.Join(failure, err)
				result.LastSuccessfulObservation = uint64(events.Unavailable)
			}
		}
		if failure != nil {
			result.State = contracts.ObservationState_Failed
			result.ExceptionMessages = []string{failure.Error()}
		}
	}()
	ctx = reactors.WithBatch(ctx, reactors.Batch{Reactor: r.plan.Identifier(), Store: r.store, Namespace: r.namespace, Sequence: r.plan.EventSequence()})
	if !r.plan.PerEvent() {
		lease, failure = r.plan.Activate(ctx)
		if failure != nil {
			return result
		}
	}
	var previous uint64
	for i, appended := range batch.Events {
		if failure = ctx.Err(); failure != nil {
			return result
		}
		if appended == nil || appended.Context == nil || (i > 0 && appended.Context.SequenceNumber <= previous) {
			failure = faults.ErrProtocol
			return result
		}
		previous = appended.Context.SequenceNumber
		var ec events.Context
		var content any
		ec, content, failure = r.decode(appended)
		if failure != nil {
			return result
		}
		deliveryCtx := invocationContext(ctx, r.plan.Identifier(), ec)
		if r.plan.PerEvent() {
			lease, failure = r.plan.Activate(deliveryCtx)
			if failure != nil {
				return result
			}
		}
		failure = lease.Invoke(deliveryCtx, content, ec, r.runtime)
		if r.plan.PerEvent() {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(deliveryCtx), 5*time.Second)
			failure = errors.Join(failure, lease.Close(cleanup))
			cancel()
			lease = nil
		}
		if failure != nil {
			return result
		}
		result.LastSuccessfulObservation = appended.Context.SequenceNumber
	}
	return result
}
func (r *Reactor) decode(appended *contracts.AppendedEvent) (events.Context, any, error) {
	ec, err := eventContext(appended.Context, r.store, r.namespace, r.plan.EventSequence())
	if err != nil {
		return ec, nil, err
	}
	descriptor, ok := r.plan.Descriptor(ec.EventType.ID)
	if !ok {
		return ec, nil, fmt.Errorf("%w: unsubscribed event type", faults.ErrProtocol)
	}
	generations := make(map[events.Generation]json.RawMessage, len(appended.GenerationalContent))
	for generation, content := range appended.GenerationalContent {
		generations[events.Generation(generation)] = json.RawMessage(content)
	}
	return DecodeContent(descriptor, ec, []byte(appended.Content), generations)
}
func invocationContext(ctx context.Context, id reactors.ID, ec events.Context) context.Context {
	ctx = metadata.WithCorrelation(ctx, ec.CorrelationID)
	actor := identities.System()
	actor.OnBehalfOf = &ec.CausedBy
	ctx = metadata.WithIdentity(ctx, actor)
	for _, cause := range ec.Causation {
		ctx = metadata.WithCausation(ctx, cause)
	}
	return metadata.WithCausation(ctx, metadata.Causation{Occurred: time.Now(), Type: "Client Reactor", Properties: map[string]string{"ReactorId": string(id), "eventTypeId": string(ec.EventType.ID), "eventTypeGeneration": strconv.FormatUint(uint64(ec.EventType.Generation), 10), "eventSequenceId": string(ec.Sequence), "eventSequenceNumber": strconv.FormatUint(uint64(ec.SequenceNumber), 10)}})
}
func eventContext(c *contracts.EventContext, store metadata.StoreName, namespace metadata.Namespace, sequence events.SequenceID) (events.Context, error) {
	if c == nil || c.EventType == nil || c.EventType.Id == "" || c.EventType.Generation == 0 || c.SequenceNumber >= uint64(events.Unavailable-2) || c.EventSourceId == "" || (c.EventStore != "" && c.EventStore != string(store)) || (c.Namespace != "" && c.Namespace != string(namespace)) {
		return events.Context{}, faults.ErrProtocol
	}
	occurred, err := time.Parse(time.RFC3339Nano, c.GetOccurred().GetValue())
	if err != nil {
		return events.Context{}, faults.ErrProtocol
	}
	ec := events.Context{Store: store, Namespace: namespace, Sequence: sequence, EventType: events.TypeRef{ID: events.TypeID(c.EventType.Id), Generation: events.Generation(c.EventType.Generation)}, Tombstone: c.EventType.Tombstone, SourceID: events.SourceID(c.EventSourceId), SourceType: events.SourceType(c.EventSourceType), StreamID: events.StreamID(c.EventStreamId), StreamType: events.StreamType(c.EventStreamType), SequenceNumber: events.SequenceNumber(c.SequenceNumber), Occurred: occurred, CorrelationID: wire.Correlation(c.CorrelationId), Subject: events.Subject(c.Subject), Hash: c.Hash, ObservationState: events.ObservationState(c.ObservationState), CausedBy: identity(c.CausedBy)}
	if ec.Subject == "" {
		ec.Subject = events.Subject(ec.SourceID)
	}
	for _, cause := range c.Causation {
		if cause == nil {
			return ec, faults.ErrProtocol
		}
		occurred, err := time.Parse(time.RFC3339Nano, cause.GetOccurred().GetValue())
		if err != nil {
			return ec, faults.ErrProtocol
		}
		properties := make(map[string]string, len(cause.Properties))
		for k, v := range cause.Properties {
			properties[k] = v
		}
		ec.Causation = append(ec.Causation, metadata.Causation{Occurred: occurred, Type: cause.Type, Properties: properties})
	}
	for _, tag := range c.Tags {
		ec.Tags = append(ec.Tags, events.Tag(tag))
	}
	for _, tag := range c.NamedTags {
		if tag == nil {
			return ec, faults.ErrProtocol
		}
		ec.NamedTags = append(ec.NamedTags, events.NamedTag{Name: tag.Name, Value: tag.Value})
	}
	return ec, nil
}
func identity(value *contracts.Identity) identities.Identity {
	if value == nil {
		return identities.NotSet()
	}
	result := identities.Identity{Subject: value.Subject, Name: value.Name, UserName: value.UserName}
	if value.OnBehalfOf != nil {
		nested := identity(value.OnBehalfOf)
		result.OnBehalfOf = &nested
	}
	return result
}
