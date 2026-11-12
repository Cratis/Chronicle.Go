// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	reactorcontracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reducers"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Reducer is the reducer protocol adapter over the shared ordered stream loop.
type Reducer struct {
	plan      *reducers.Plan
	stream    grpc.BidiStreamingClient[contracts.ReducerMessage, contracts.ReduceOperationMessage]
	store     metadata.StoreName
	namespace metadata.Namespace
	// OnChange receives successful local folds before kernel acknowledgement.
	// Install before Run. It must not block or call user code.
	OnChange func(string, json.RawMessage)
	// OnDelivered runs on the Run goroutine for every message the kernel
	// delivers. Install before Run. It must not block or call user code.
	OnDelivered func()
}

// OpenReducer sends the immutable registration snapshot. Like reactors, the
// duplex protocol has no registration acknowledgement beyond a successful send.
// Failures to open or register are StreamErrors.
func OpenReducer(ctx context.Context, conn grpc.ClientConnInterface, connectionID string, store metadata.StoreName, namespace metadata.Namespace, plan *reducers.Plan) (*Reducer, error) {
	stream, err := contracts.NewReducersClient(conn).Observe(ctx)
	if err != nil {
		return nil, streamError(err)
	}
	definition := &contracts.ReducerDefinition{ReducerId: string(plan.Identifier()), EventSequenceId: string(plan.EventSequence()), ReadModel: string(plan.Model().Identifier()), IsActive: plan.IsActive(), Tags: plan.Tags(), Filters: &contracts.ObserverFilters{FilterTags: plan.FilterTags(), EventSourceType: string(plan.EventSourceType()), EventStreamType: string(plan.EventStreamType())}, Hash: plan.Fingerprint()}
	for _, ref := range plan.EventTypes() {
		definition.EventTypes = append(definition.EventTypes, &contracts.EventTypeWithKeyExpression{EventType: &contracts.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)}, Key: "$eventSourceId"})
	}
	if err = sendOnStream(stream.Send, stream.Recv, &contracts.ReducerMessage{Content: &contracts.OneOf_RegisterReducer_ReducerResult{Value0: &contracts.RegisterReducer{ConnectionId: connectionID, EventStore: string(store), Namespace: string(namespace), Reducer: definition, PageDelivery: &contracts.PageDelivery{Enabled: true}}}}); err != nil {
		return nil, err
	}
	return &Reducer{plan: plan, stream: stream, store: store, namespace: namespace}, nil
}

// Run processes one complete fold and cleanup before acknowledging it. Replay
// notifications run serially in fresh scopes and have no result handshake.
func (r *Reducer) Run(ctx context.Context) error {
	return runStream(ctx, r.stream.Recv, r.OnDelivered, func(ctx context.Context, operation *contracts.ReduceOperationMessage) (*contracts.ReducerResult, error) {
		if operation.ReplayState != contracts.ReplayState_REPLAY_STATE_None {
			var state reducers.ReplayState
			switch operation.ReplayState {
			case contracts.ReplayState_BeginReplay:
				state = reducers.BeginReplay
			case contracts.ReplayState_EndReplay:
				state = reducers.EndReplay
			case contracts.ReplayState_BeginReplayPartition:
				state = reducers.BeginReplayPartition
			case contracts.ReplayState_EndReplayPartition:
				state = reducers.EndReplayPartition
			default:
				return nil, fmt.Errorf("%w: unknown reducer replay state %d", faults.ErrProtocol, operation.ReplayState)
			}
			ctx = reducers.WithBatch(ctx, reducers.Batch{Reducer: r.plan.Identifier(), Store: r.store, Namespace: r.namespace, Sequence: r.plan.EventSequence()})
			return nil, r.plan.NotifyReplay(ctx, state, events.SourceID(operation.Partition))
		}
		return r.handle(ctx, operation), nil
	}, func(result *contracts.ReducerResult) error {
		return r.stream.Send(&contracts.ReducerMessage{Content: &contracts.OneOf_RegisterReducer_ReducerResult{Value1: result}})
	})
}
func (r *Reducer) handle(ctx context.Context, operation *contracts.ReduceOperationMessage) *contracts.ReducerResult {
	paged := len(operation.Partitions) > 0
	raw := operation.Partitions
	if !paged {
		raw = []*contracts.ReducerPartition{{Partition: operation.Partition, InitialState: operation.InitialState, Events: operation.Events}}
	}
	ctx = reducers.WithBatch(ctx, reducers.Batch{Reducer: r.plan.Identifier(), Store: r.store, Namespace: r.namespace, Sequence: r.plan.EventSequence()})
	decoded := make([]decodedReducerPartition, len(raw))
	described := make([]PagePartition, len(raw))
	for i, partition := range raw {
		decoded[i] = r.decodePartition(partition)
		described[i] = PagePartition{Partition: decoded[i].partition}
		for _, event := range decoded[i].events {
			described[i].Events = append(described[i].Events, PageEvent{Content: event.Content, Context: event.Context})
		}
	}
	batchContext := NewBatchContext(string(r.plan.Identifier()), r.store, r.namespace, r.plan.EventSequence(), reducerOrigin(operation.Origin), described, time.Duration(operation.DeliverWithinMilliseconds)*time.Millisecond, paged)
	outcomes := make([]PartitionOutcome, len(decoded))
	limit := make(chan struct{}, r.plan.MaxConcurrency())
	var wg sync.WaitGroup
	for i := range decoded {
		wg.Add(1)
		limit <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-limit }()
			outcomes[i] = r.reducePartition(ctx, decoded[i], batchContext)
		}()
	}
	wg.Wait()
	for _, outcome := range outcomes {
		if outcome.State == reactorcontracts.ObservationState_Success && r.OnChange != nil {
			var state json.RawMessage
			if outcome.ReadModelState != "" {
				state = json.RawMessage(outcome.ReadModelState)
			}
			r.OnChange(outcome.Partition, state)
		}
	}
	if paged {
		result := &contracts.ReducerResult{DeliveryId: operation.DeliveryId}
		for _, outcome := range outcomes {
			result.Partitions = append(result.Partitions, &contracts.PartitionResult{Partition: outcome.Partition, State: contracts.ObservationState(outcome.State), LastSuccessfulObservation: uint64(outcome.LastSuccessful), ExceptionMessages: messages(outcome.Err), RetryAfterMilliseconds: uint64(outcome.RetryAfter.Milliseconds()), ReadModelState: outcome.ReadModelState})
		}
		return result
	}
	outcome := outcomes[0]
	result := &contracts.ReducerResult{Partition: operation.Partition, DeliveryId: operation.DeliveryId, State: contracts.ObservationState(outcome.State), LastSuccessfulObservation: uint64(outcome.LastSuccessful), ExceptionMessages: messages(outcome.Err), ReadModelState: outcome.ReadModelState}
	if result.State == contracts.ObservationState_Deferred {
		// A kernel that does not deliver pages does not understand Deferred.
		result.State = contracts.ObservationState_Failed
	}
	return result
}

type decodedReducerPartition struct {
	partition string
	initial   any
	events    []reducers.Event
	failure   error
}

func (r *Reducer) decodePartition(partition *contracts.ReducerPartition) (decoded decodedReducerPartition) {
	if partition == nil {
		decoded.failure = faults.ErrProtocol
		return decoded
	}
	decoded.partition = partition.Partition
	data := bytes.TrimSpace([]byte(partition.InitialState))
	if len(data) != 0 && !bytes.Equal(data, []byte("null")) {
		if data[0] != '{' {
			decoded.failure = faults.ErrProtocol
			return decoded
		}
		value, err := r.plan.Model().Unmarshal(data)
		if err != nil {
			decoded.failure = faults.ErrProtocol
			return decoded
		}
		decoded.initial = value
	}
	for _, appended := range partition.Events {
		event, err := r.decode(appended)
		if err != nil {
			decoded.failure = err
			return decoded
		}
		decoded.events = append(decoded.events, event)
	}
	return decoded
}

// reducePartition folds one partition in its own scope. A reducer writes no
// state for a failed or deferred partition, so nothing before the problem counts
// as handled unless the fold itself reported progress.
func (r *Reducer) reducePartition(ctx context.Context, partition decodedReducerPartition, batchContext observation.BatchContext) (outcome PartitionOutcome) {
	outcome = PartitionOutcome{Partition: partition.partition, State: reactorcontracts.ObservationState_Success, LastSuccessful: events.Unavailable}
	defer func() {
		if recovered := recover(); recovered != nil {
			outcome = PartitionOutcome{Partition: partition.partition, State: reactorcontracts.ObservationState_Failed, LastSuccessful: events.Unavailable, Err: &diagnostics.PanicError{}}
			diagnostics.Log(ctx, r.plan.Logger(), slog.LevelError, "reducer operation failed", "reducer", "observe", outcome.Err)
		}
	}()
	if partition.failure != nil {
		diagnostics.Log(ctx, r.plan.Logger(), slog.LevelError, "reducer operation failed", "reducer", "observe", partition.failure)
		outcome.State, outcome.Err = reactorcontracts.ObservationState_Failed, partition.failure
		return outcome
	}
	folded := r.plan.Reduce(ctx, partition.events, partition.initial, reducers.WithBatchContext(batchContext))
	outcome.LastSuccessful = folded.LastSuccessful
	// A type assertion, not errors.As: the runtime never calls into application errors.
	deferred, isDeferred := folded.Err.(*observation.DeferredError)
	switch {
	case isDeferred:
		outcome.State, outcome.RetryAfter, outcome.LastSuccessful = reactorcontracts.ObservationState_Deferred, deferred.RetryAfter, events.Unavailable
		return outcome
	case folded.Err != nil:
		// Reduce already records its operation failure.
		outcome.State, outcome.Err = reactorcontracts.ObservationState_Failed, folded.Err
		return outcome
	case folded.State == nil:
		return outcome
	}
	encoded, err := r.plan.Model().Marshal(folded.State)
	if err != nil {
		diagnostics.Log(ctx, r.plan.Logger(), slog.LevelError, "reducer operation failed", "reducer", "observe", err)
		outcome.State, outcome.Err = reactorcontracts.ObservationState_Failed, err
		return outcome
	}
	outcome.ReadModelState = string(encoded)
	return outcome
}

func reducerOrigin(value contracts.ObservationOrigin) observation.Origin {
	return origin(reactorcontracts.ObservationOrigin(value))
}
func (r *Reducer) decode(appended *contracts.AppendedEvent) (reducers.Event, error) {
	if appended == nil || appended.Context == nil {
		return reducers.Event{}, faults.ErrProtocol
	}
	// protobuf-net generates duplicate EventContext messages in each service.
	// Their field numbers/types are identical; normalize once at this boundary to
	// reuse the reactor runtime's strict metadata validation, not a second decoder.
	encoded, err := proto.Marshal(appended.Context)
	if err != nil {
		return reducers.Event{}, faults.ErrProtocol
	}
	normalized := &reactorcontracts.EventContext{}
	if err := proto.Unmarshal(encoded, normalized); err != nil {
		return reducers.Event{}, faults.ErrProtocol
	}
	ec, err := eventContext(normalized, r.store, r.namespace, r.plan.EventSequence())
	if err != nil {
		return reducers.Event{}, err
	}
	descriptor, ok := r.plan.Descriptor(ec.EventType.ID)
	if !ok {
		return reducers.Event{}, fmt.Errorf("%w: unsubscribed reducer event", faults.ErrProtocol)
	}
	generations := make(map[events.Generation]json.RawMessage, len(appended.GenerationalContent))
	for generation, content := range appended.GenerationalContent {
		generations[events.Generation(generation)] = json.RawMessage(content)
	}
	ec, value, err := DecodeContent(descriptor, ec, []byte(appended.Content), generations)
	return reducers.Event{Content: value, Context: ec}, err
}
