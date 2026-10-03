// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

func (s *EventStore) readModelReactorPlans() []*reactors.ReadModelPlan {
	if plans, ok := s.client.readModelReactors.stores[s.name]; ok {
		return plans
	}
	return s.client.readModelReactors.defaults
}
func (s *EventStore) startReadModelReactors(ctx context.Context, g *generation, waitReady bool) error {
	var plans []observerPlan
	for _, plan := range s.readModelReactorPlans() {
		plans = append(plans, observerPlan{id: string(plan.Identifier()), oneShot: true, open: func(ctx context.Context, g *generation) (observerStream, error) {
			// The generation transport avoids recursively entering registration from
			// the watch ready barrier. Scope/effects still use the public store runtime.
			service, err := readmodels.New(s.name, s.namespace, s.readModels.Catalog(), g.transport, readmodels.WithReductionChanges(&s.readModelChanges))
			if err != nil {
				return nil, err
			}
			run := &readModelReactorRun{store: s, plan: plan}
			if plan.IsMaterialized() {
				run.windows, err = service.Materialized().ObserveInstances(ctx, plan.Model().Identifier(), plan.Window(), plan.WatchOptions()...)
			} else {
				run.changes, err = service.Watch(ctx, plan.Model().Identifier(), plan.WatchOptions()...)
			}
			if err != nil {
				plan.Report(ctx, err)
				return nil, err
			}
			return run, nil
		}})
	}
	return s.readModelReactors.start(ctx, g, waitReady, plans, s.client.config.reactorRetryWait)
}

// UnregisterReadModelReactor cancels and joins current and retired generation
// callbacks for this namespace. Removal survives reconnect. Do not call it from
// its own callback; cancellation cannot forcibly stop uncooperative user code.
func (s *EventStore) UnregisterReadModelReactor(ctx context.Context, id reactors.ID) error {
	return s.readModelReactors.unregister(ctx, string(id))
}

type readModelReactorRun struct {
	store   *EventStore
	plan    *reactors.ReadModelPlan
	changes *readmodels.Subscription[readmodels.Change[json.RawMessage]]
	windows *readmodels.Subscription[[]json.RawMessage]
}

func (r *readModelReactorRun) Run(ctx context.Context) (err error) {
	defer func() {
		if err != nil && ctx.Err() == nil {
			r.plan.Report(ctx, err)
		}
	}()
	if r.changes != nil {
		defer func() { _ = r.changes.Close() }() // Subscription cleanup only cancels and joins.
		seen := make(map[readmodels.Key]bool)
		kind, _ := r.plan.Model().Observer()
		for {
			change, err := r.changes.Recv()
			if err != nil {
				return err
			}
			// Dispatch reports every callback failure and still runs other matches.
			// Model-change callbacks, unlike event reactors, have no durable retry.
			if kind == readmodels.Reducer {
				if change.Type == readmodels.Removed {
					delete(seen, change.Key)
				} else {
					if !seen[change.Key] {
						change.Type = readmodels.Added
					}
					seen[change.Key] = true
				}
			}
			_ = r.plan.Dispatch(observerruntime.ReactorInvocationContext(ctx, r.plan.Identifier(), change.Context), change, reactorStoreRuntime{r.store})
		}
	}
	defer func() { _ = r.windows.Close() }() // Subscription cleanup only cancels and joins.
	differ := &readmodels.WindowDiffer{}
	for {
		window, err := r.windows.Recv()
		if err != nil {
			return err
		}
		changes, err := differ.Diff(r.plan.Model(), window)
		if err != nil {
			return err
		}
		for _, change := range changes {
			change.Context = events.Context{Store: r.store.name, Namespace: r.store.namespace, Sequence: r.plan.Model().EventSequence(), SourceID: events.SourceID(change.Key), SourceType: events.DefaultSourceType, StreamType: events.AllStreamTypes, StreamID: events.DefaultStreamID, SequenceNumber: events.Unavailable}
			_ = r.plan.Dispatch(observerruntime.ReactorInvocationContext(ctx, r.plan.Identifier(), change.Context), change, reactorStoreRuntime{r.store})
		}
	}
}
