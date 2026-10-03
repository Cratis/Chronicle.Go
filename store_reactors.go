// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"reflect"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

func (s *EventStore) reactorPlans() []*reactors.Plan {
	if plans, ok := s.client.reactors.stores[s.name]; ok {
		return plans
	}
	return s.client.reactors.defaults
}
func (s *EventStore) startReactors(ctx context.Context, g *generation, waitReady bool) error {
	var plans []observerPlan
	for _, plan := range s.reactorPlans() {
		plans = append(plans, observerPlan{string(plan.Identifier()), func(ctx context.Context, g *generation) (observerStream, error) {
			return observerruntime.Open(ctx, g.transport, g.id, s.name, s.namespace, plan, reactorStoreRuntime{s})
		}})
	}
	return s.reactors.start(ctx, g, waitReady, plans, s.client.config.reactorRetryWait)
}

// UnregisterReactor disconnects and joins this store/namespace's reactor across
// current and retired generations. Unknown IDs are ignored. It retains the
// removal across reconnect; it does not remove persisted kernel state. Reconnect
// may overlap old and new generation delivery until old callbacks return, as in
// C#. A context error means cleanup is still joining. Do not synchronously
// unregister this reactor from its own callback.
func (s *EventStore) UnregisterReactor(ctx context.Context, id reactors.ID) error {
	for _, plan := range s.reactorPlans() {
		if plan.Identifier() == id {
			return s.reactors.unregister(ctx, string(id))
		}
	}
	return nil
}

type reactorStoreRuntime struct{ store *EventStore }

func (r reactorStoreRuntime) Append(ctx context.Context, source events.SourceID, value any) error {
	result, err := r.store.EventLog().Append(ctx, source, value)
	if err != nil {
		return err
	}
	return result.Err()
}
func (r reactorStoreRuntime) ReadModel(ctx context.Context, _ readmodels.Descriptor, key readmodels.Key, typ reflect.Type) (any, error) {
	return r.store.ReadModels().GetValue(ctx, typ, key)
}
