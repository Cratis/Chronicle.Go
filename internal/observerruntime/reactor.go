// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package observerruntime owns ordered observer delivery and acknowledgement.
package observerruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reactors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
)

// Reactor owns a single duplex stream. Run has one receiver and one sender with
// no application queue: transport backpressure bounds pending messages.
type Reactor struct {
	plan      *reactors.Plan
	stream    grpc.BidiStreamingClient[contracts.ReactorMessage, contracts.EventsToObserve]
	runtime   reactors.Runtime
	store     metadata.StoreName
	namespace metadata.Namespace
	// OnDelivered runs on the Run goroutine for every message the kernel
	// delivers. Install before Run. It must not block or call user code.
	OnDelivered func()
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
		IsReplayable: plan.IsReplayable(), StartAtTail: plan.StartsAtTail(), Tags: plan.Tags(),
		Filters: &contracts.ObserverFilters{FilterTags: plan.FilterTags(), EventSourceType: string(plan.EventSourceType()), EventStreamType: string(plan.EventStreamType())},
	}
	// C# initializes an absent field 4 to true (DefaultValue(true)). As for
	// webhook registration, preserve explicit false on this fresh wire snapshot
	// without changing the authoritative proto3 contracts or the frozen plan.
	if !definition.IsReplayable {
		definition.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 4, protowire.VarintType), 0))
	}
	for _, ref := range plan.EventTypes() {
		definition.EventTypes = append(definition.EventTypes, &contracts.EventTypeWithKeyExpression{EventType: &contracts.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)}, Key: "$eventSourceId"})
	}
	err = stream.Send(&contracts.ReactorMessage{Content: &contracts.OneOf_RegisterReactor_ReactorResult{Value0: &contracts.RegisterReactor{ConnectionId: connectionID, EventStore: string(store), Namespace: string(namespace), Reactor: definition, PageDelivery: &contracts.PageDelivery{Enabled: true}}}})
	if err != nil {
		return nil, err
	}
	return &Reactor{plan: plan, stream: stream, runtime: runtime, store: store, namespace: namespace}, nil
}

// Run processes batches sequentially until cancellation or stream failure. Effects
// are never retried here. The kernel controls failure recovery and resume position.
func (r *Reactor) Run(ctx context.Context) error {
	return runStream(ctx, delivering(r.stream.Recv, r.OnDelivered), func(ctx context.Context, batch *contracts.EventsToObserve) (*contracts.ReactorResult, error) {
		if batch.ReplayState != contracts.ReplayState_REPLAY_STATE_None {
			ctx = reactors.WithBatch(ctx, reactors.Batch{Reactor: r.plan.Identifier(), Store: r.store, Namespace: r.namespace, Sequence: r.plan.EventSequence()})
			return nil, r.plan.NotifyReplay(ctx, reactors.ReplayState(batch.ReplayState), events.SourceID(batch.Partition))
		}
		return r.handle(ctx, batch), nil
	}, func(result *contracts.ReactorResult) error {
		return r.stream.Send(&contracts.ReactorMessage{Content: &contracts.OneOf_RegisterReactor_ReactorResult{Value1: result}})
	})
}
func (r *Reactor) handle(ctx context.Context, batch *contracts.EventsToObserve) (result *contracts.ReactorResult) {
	paged := len(batch.Partitions) > 0
	raw := batch.Partitions
	if !paged {
		raw = []*contracts.PartitionEvents{{Partition: batch.Partition, Events: batch.Events}}
	}
	partitions := make([]PagePartition, 0, len(raw))
	for _, partition := range raw {
		if partition == nil {
			partitions = append(partitions, PagePartition{Failed: faults.ErrProtocol, FailedAt: 0})
			continue
		}
		partitions = append(partitions, r.decodePartition(partition.Partition, partition.Events))
	}
	ctx = reactors.WithBatch(ctx, reactors.Batch{Reactor: r.plan.Identifier(), Store: r.store, Namespace: r.namespace, Sequence: r.plan.EventSequence()})
	batchContext := NewBatchContext(string(r.plan.Identifier()), r.store, r.namespace, r.plan.EventSequence(), origin(batch.Origin), partitions, time.Duration(batch.DeliverWithinMilliseconds)*time.Millisecond, paged)
	outcomes := DispatchReactorPage(ctx, r.plan, partitions, batchContext, r.runtime)
	if paged {
		result = &contracts.ReactorResult{DeliveryId: batch.DeliveryId}
		for _, outcome := range outcomes {
			result.Partitions = append(result.Partitions, &contracts.PartitionResult{Partition: outcome.Partition, State: outcome.State, LastSuccessfulObservation: uint64(outcome.LastSuccessful), ExceptionMessages: messages(outcome.Err), RetryAfterMilliseconds: uint64(outcome.RetryAfter.Milliseconds())})
		}
		return result
	}
	outcome := outcomes[0]
	result = &contracts.ReactorResult{Partition: batch.Partition, DeliveryId: batch.DeliveryId, State: outcome.State, LastSuccessfulObservation: uint64(outcome.LastSuccessful), ExceptionMessages: messages(outcome.Err)}
	if result.State == contracts.ObservationState_Deferred {
		// A kernel that does not deliver pages does not understand Deferred.
		result.State = contracts.ObservationState_Failed
	}
	return result
}

// decodePartition validates and decodes one partition's events, stopping at the
// first event that cannot be decoded. Events before it can still be handled.
func (r *Reactor) decodePartition(partition string, appended []*contracts.AppendedEvent) PagePartition {
	decoded := PagePartition{Partition: partition}
	var previous uint64
	for i, event := range appended {
		at := events.SequenceNumber(previous + 1)
		if i == 0 {
			at = 0
		}
		if event == nil || event.Context == nil || (i > 0 && event.Context.SequenceNumber <= previous) {
			decoded.Failed, decoded.FailedAt = faults.ErrProtocol, at
			return decoded
		}
		previous = event.Context.SequenceNumber
		ec, content, err := r.decode(event)
		if err != nil {
			decoded.Failed, decoded.FailedAt = err, events.SequenceNumber(event.Context.SequenceNumber)
			return decoded
		}
		decoded.Events = append(decoded.Events, PageEvent{Content: content, Context: ec})
	}
	return decoded
}

func messages(err error) []string {
	if err == nil {
		return nil
	}
	return []string{err.Error()}
}

func origin(value contracts.ObservationOrigin) observation.Origin {
	switch value {
	case contracts.ObservationOrigin_Live:
		return observation.OriginLive
	case contracts.ObservationOrigin_CatchingUp:
		return observation.OriginCatchUp
	case contracts.ObservationOrigin_Replaying:
		return observation.OriginReplay
	case contracts.ObservationOrigin_Retrying:
		return observation.OriginRetry
	default:
		return observation.OriginUnknown
	}
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

// ReactorInvocationContext applies the production identity and causation scope.
// Scenario adapters share it rather than defining test-only ambient semantics.
func ReactorInvocationContext(ctx context.Context, id reactors.ID, ec events.Context) context.Context {
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
	ec := events.Context{Store: store, Namespace: namespace, Sequence: sequence, EventType: events.TypeRef{ID: events.TypeID(c.EventType.Id), Generation: events.Generation(c.EventType.Generation)}, Tombstone: c.EventType.Tombstone, SourceID: events.SourceID(c.EventSourceId), SourceType: events.SourceType(c.EventSourceType), StreamID: events.StreamID(c.EventStreamId), StreamType: events.StreamType(c.EventStreamType), SequenceNumber: events.SequenceNumber(c.SequenceNumber), Occurred: occurred, CorrelationID: wire.Correlation(c.CorrelationId), Subject: events.Subject(c.Subject), Hash: c.Hash, ObservationState: events.ObservationState(c.ObservationState), CompensationFor: events.CompensationTarget(c.CompensationFor), CausedBy: identity(c.CausedBy)}
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
