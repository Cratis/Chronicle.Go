// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"

	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

func (s *EventStore) reducerPlans() []*reducers.Plan {
	if s.reducerSnapshot != nil {
		return s.reducerSnapshot
	}
	if plans, ok := s.client.reducers.stores[s.name]; ok {
		return plans
	}
	return s.client.reducers.defaults
}
func (s *EventStore) startReducers(ctx context.Context, g *generation, waitReady bool) error {
	s.readModelChanges.BindGeneration(g.ctx)
	var plans []observerPlan
	for _, plan := range s.reducerPlans() {
		plans = append(plans, observerPlan{id: string(plan.Identifier()), logger: plan.Logger(), operation: "reducer", open: func(ctx context.Context, g *generation) (observerStream, error) {
			runtime, err := observerruntime.OpenReducer(ctx, g.transport, g.id, s.name, s.namespace, plan)
			if err == nil {
				runtime.OnChange = func(key string, value json.RawMessage) {
					s.readModelChanges.Publish(g.ctx, plan.Model().Identifier(), readmodels.Key(key), value)
				}
			}
			return runtime, err
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
