// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

func (s *EventStore) reducerPlans() []*reducers.Plan {
	if plans, ok := s.client.reducers.stores[s.name]; ok {
		return plans
	}
	return s.client.reducers.defaults
}
func (s *EventStore) startReducers(ctx context.Context, g *generation, waitReady bool) error {
	var plans []observerPlan
	for _, plan := range s.reducerPlans() {
		plans = append(plans, observerPlan{string(plan.Identifier()), func(ctx context.Context, g *generation) (observerStream, error) {
			return observerruntime.OpenReducer(ctx, g.transport, g.id, s.name, s.namespace, plan)
		}})
	}
	return s.reducers.start(ctx, g, waitReady, plans, s.client.config.reactorRetryWait)
}

// UnregisterReducer cancels and joins this store/namespace's reducer across
// current and retired generations. Unknown IDs are ignored. Removal survives
// reconnect; persisted kernel definitions are not deleted. A canceled wait does
// not abandon cleanup. Never call synchronously from this reducer's own fold.
// Passive reads remain available from the frozen plan after stream removal.
func (s *EventStore) UnregisterReducer(ctx context.Context, id reducers.ID) error {
	for _, plan := range s.reducerPlans() {
		if plan.Identifier() == id {
			return s.reducers.unregister(ctx, string(id))
		}
	}
	return nil
}
func (s *EventStore) readPassiveReducer(ctx context.Context, model readmodels.Descriptor, key readmodels.Key) (readmodels.Instance[json.RawMessage], error) {
	empty := readmodels.Instance[json.RawMessage]{}
	var plan *reducers.Plan
	for _, candidate := range s.reducerPlans() {
		if candidate.Model().Identifier() == model.Identifier() {
			plan = candidate
			break
		}
	}
	if plan == nil || !plan.IsPassive() {
		return empty, fmt.Errorf("%w: passive reducer not registered", ErrNotRegistered)
	}
	sequence, err := s.EventSequence(plan.EventSequence())
	if err != nil {
		return empty, err
	}
	history, err := sequence.ReadSource(ctx, events.SourceID(key), eventsequences.SourceFilter{EventTypes: plan.EventTypes()})
	if err != nil {
		return empty, err
	}
	// C# passive reads request source/type history, not the active observer's
	// metadata filters. Keep that contract; filters govern kernel delivery only.
	batch := make([]reducers.Event, 0, len(history))
	for _, appended := range history {
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
	if len(batch) == 0 {
		return empty, nil
	}
	slices.SortFunc(batch, func(a, b reducers.Event) int {
		if a.Context.SequenceNumber < b.Context.SequenceNumber {
			return -1
		}
		if a.Context.SequenceNumber > b.Context.SequenceNumber {
			return 1
		}
		return 0
	})
	ctx = reducers.WithBatch(ctx, reducers.Batch{Reducer: plan.Identifier(), Store: s.name, Namespace: s.namespace, Sequence: plan.EventSequence()})
	result := plan.Reduce(ctx, batch, nil)
	if result.Err != nil {
		return empty, result.Err
	}
	instance := empty
	if result.LastSuccessful != events.Unavailable {
		instance.LastHandled = &result.LastSuccessful
	}
	if result.State != nil {
		instance.Value, err = model.Marshal(result.State)
		if err != nil {
			return empty, err
		}
		instance.Exists = true
	}
	return instance, nil
}
