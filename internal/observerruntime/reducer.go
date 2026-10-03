// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	reactorcontracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
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
}

// OpenReducer sends the immutable registration snapshot. Like reactors, the
// duplex protocol has no registration acknowledgement beyond a successful send.
func OpenReducer(ctx context.Context, conn grpc.ClientConnInterface, connectionID string, store metadata.StoreName, namespace metadata.Namespace, plan *reducers.Plan) (*Reducer, error) {
	stream, err := contracts.NewReducersClient(conn).Observe(ctx)
	if err != nil {
		return nil, err
	}
	definition := &contracts.ReducerDefinition{ReducerId: string(plan.Identifier()), EventSequenceId: string(plan.EventSequence()), ReadModel: string(plan.Model().Identifier()), IsActive: plan.IsActive(), Tags: plan.Tags(), Filters: &contracts.ObserverFilters{FilterTags: plan.FilterTags(), EventSourceType: string(plan.EventSourceType()), EventStreamType: string(plan.EventStreamType())}, Hash: plan.Fingerprint()}
	for _, ref := range plan.EventTypes() {
		definition.EventTypes = append(definition.EventTypes, &contracts.EventTypeWithKeyExpression{EventType: &contracts.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)}, Key: "$eventSourceId"})
	}
	if err = stream.Send(&contracts.ReducerMessage{Content: &contracts.OneOf_RegisterReducer_ReducerResult{Value0: &contracts.RegisterReducer{ConnectionId: connectionID, EventStore: string(store), Namespace: string(namespace), Reducer: definition}}}); err != nil {
		return nil, err
	}
	return &Reducer{plan, stream, store, namespace}, nil
}

// Run processes one complete fold and cleanup before acknowledging it. Replay
// notifications have no handshake; user replay hooks remain out of scope.
func (r *Reducer) Run(ctx context.Context) error {
	return runStream(ctx, r.stream.Recv, func(ctx context.Context, operation *contracts.ReduceOperationMessage) (*contracts.ReducerResult, error) {
		if operation.ReplayState != contracts.ReplayState_REPLAY_STATE_None {
			return nil, nil
		}
		return r.handle(ctx, operation), nil
	}, func(result *contracts.ReducerResult) error {
		return r.stream.Send(&contracts.ReducerMessage{Content: &contracts.OneOf_RegisterReducer_ReducerResult{Value1: result}})
	})
}
func (r *Reducer) handle(ctx context.Context, operation *contracts.ReduceOperationMessage) (result *contracts.ReducerResult) {
	result = &contracts.ReducerResult{Partition: operation.Partition, State: contracts.ObservationState_Success, LastSuccessfulObservation: uint64(events.Unavailable)}
	var failure error
	defer func() {
		if recovered := recover(); recovered != nil {
			failure = fmt.Errorf("reducer operation panic: %v", recovered)
		}
		if failure != nil {
			result.State = contracts.ObservationState_Failed
			result.ReadModelState = ""
			result.ExceptionMessages = []string{failure.Error()}
		}
	}()
	ctx = reducers.WithBatch(ctx, reducers.Batch{Reducer: r.plan.Identifier(), Store: r.store, Namespace: r.namespace, Sequence: r.plan.EventSequence()})
	var initial any
	data := bytes.TrimSpace([]byte(operation.InitialState))
	if len(data) != 0 && !bytes.Equal(data, []byte("null")) {
		if data[0] != '{' {
			failure = faults.ErrProtocol
			return result
		}
		value := reflect.New(r.plan.Model().GoType())
		if err := json.Unmarshal(data, value.Interface()); err != nil {
			failure = faults.ErrProtocol
			return result
		}
		initial = value.Interface()
	}
	batch := make([]reducers.Event, 0, len(operation.Events))
	for _, appended := range operation.Events {
		var event reducers.Event
		event, failure = r.decode(appended)
		if failure != nil {
			return result
		}
		batch = append(batch, event)
	}
	folded := r.plan.Reduce(ctx, batch, initial)
	result.LastSuccessfulObservation = uint64(folded.LastSuccessful)
	failure = folded.Err
	if failure != nil || folded.State == nil {
		return result
	}
	encoded, err := r.plan.Model().Marshal(folded.State)
	if err != nil {
		failure = err
		return result
	}
	result.ReadModelState = string(encoded)
	return result
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
