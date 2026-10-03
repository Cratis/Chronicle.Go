// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

func (s *EventStore) reducerReadPlan(model readmodels.Descriptor) (*reducers.Plan, error) {
	for _, plan := range s.reducerPlans() {
		if plan.Model().Identifier() == model.Identifier() {
			return plan, nil
		}
	}
	return nil, fmt.Errorf("%w: local reducer plan required", ErrUnsupported)
}

func (s *EventStore) readPassiveReducer(ctx context.Context, model readmodels.Descriptor, key readmodels.Key) (readmodels.Instance[json.RawMessage], error) {
	empty := readmodels.Instance[json.RawMessage]{}
	plan, err := s.reducerReadPlan(model)
	if err != nil {
		return empty, err
	}
	if !plan.IsPassive() {
		return empty, ErrUnsupported
	}
	sequence, err := s.EventSequence(plan.EventSequence())
	if err != nil {
		return empty, err
	}
	// C# reads source/type history; active observer metadata filters do not
	// constrain historical queries. Sequence validates, sorts and rejects duplicates.
	history, err := sequence.ReadSource(ctx, events.SourceID(key), eventsequences.SourceFilter{EventTypes: plan.EventTypes()})
	if err != nil {
		return empty, err
	}
	return s.foldReducerHistory(ctx, plan, model, history)
}

func (s *EventStore) readReducerCollection(ctx context.Context, model readmodels.Descriptor, count events.Count) (readmodels.Collection[json.RawMessage], error) {
	empty := readmodels.Collection[json.RawMessage]{}
	plan, err := s.reducerReadPlan(model)
	if err != nil {
		return empty, err
	}
	sequence, err := s.EventSequence(plan.EventSequence())
	if err != nil {
		return empty, err
	}
	history, err := sequence.ReadFrom(ctx, 0, eventsequences.FromFilter{EventTypes: plan.EventTypes()})
	if err != nil {
		return empty, err
	}
	if count != events.UnlimitedCount && uint64(len(history)) > uint64(count) {
		history = history[:int(count)]
	}
	// Bound globally before grouping. Sparse sequence positions are not counts.
	groups := make(map[events.SourceID][]events.Appended)
	order := make([]events.SourceID, 0)
	for _, event := range history {
		if _, exists := groups[event.Context.SourceID]; !exists {
			order = append(order, event.Context.SourceID)
		}
		groups[event.Context.SourceID] = append(groups[event.Context.SourceID], event)
	}
	result := readmodels.Collection[json.RawMessage]{Instances: []readmodels.Instance[json.RawMessage]{}, ProcessedEventsCount: events.Count(len(history))}
	for _, source := range order {
		instance, err := s.foldReducerHistory(ctx, plan, model, groups[source])
		if err != nil {
			return empty, err
		}
		if instance.Exists {
			result.Instances = append(result.Instances, instance)
		}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return result, nil
}

func (s *EventStore) foldReducerHistory(ctx context.Context, plan *reducers.Plan, model readmodels.Descriptor, history []events.Appended) (readmodels.Instance[json.RawMessage], error) {
	empty := readmodels.Instance[json.RawMessage]{}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if len(history) == 0 {
		return empty, nil
	}
	batch := make([]reducers.Event, 0, len(history))
	for _, appended := range history {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		descriptor, ok := plan.Descriptor(appended.Context.EventType.ID)
		if !ok {
			return empty, ErrProtocol
		}
		ec, content, err := observerruntime.DecodeContent(descriptor, appended.Context, appended.Content, appended.GenerationalContent)
		if err != nil {
			return empty, err
		}
		batch = append(batch, reducers.Event{Content: content, Context: ec})
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	ctx = reducers.WithBatch(ctx, reducers.Batch{Reducer: plan.Identifier(), Store: s.name, Namespace: s.namespace, Sequence: plan.EventSequence()})
	result := plan.Reduce(ctx, batch, nil, reducers.WithCallerIdentity())
	if result.Err != nil {
		return empty, result.Err
	}
	instance := empty
	if result.LastSuccessful != events.Unavailable {
		instance.LastHandled = &result.LastSuccessful
	}
	if result.State != nil {
		var err error
		instance.Value, err = model.Marshal(result.State)
		if err != nil {
			return empty, err
		}
		instance.Exists = true
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return instance, nil
}
