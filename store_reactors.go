// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"log/slog"
	"reflect"
	"sync"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

type storeReactors struct {
	mu      sync.Mutex
	runs    map[reactors.ID]*reactorRun
	removed map[reactors.ID]bool
}
type reactorRun struct {
	generation uint64
	cancel     context.CancelFunc
	ready      chan struct{}
	done       chan struct{}
	err        error
}

func (s *EventStore) reactorPlans() []*reactors.Plan {
	if plans, ok := s.client.reactors.stores[s.name]; ok {
		return plans
	}
	return s.client.reactors.defaults
}
func (s *EventStore) startReactors(ctx context.Context, g *generation) error {
	for _, plan := range s.reactorPlans() {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.reactors.mu.Lock()
		if s.reactors.removed[plan.Identifier()] {
			s.reactors.mu.Unlock()
			continue
		}
		if run := s.reactors.runs[plan.Identifier()]; run != nil && run.generation == g.number {
			s.reactors.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-run.ready:
				if run.err != nil {
					return run.err
				}
			}
			continue
		}
		runCtx, cancel := context.WithCancel(g.ctx)
		run := &reactorRun{generation: g.number, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}
		if s.reactors.runs == nil {
			s.reactors.runs = make(map[reactors.ID]*reactorRun)
		}
		s.reactors.runs[plan.Identifier()] = run
		// The caller is either admitted in g.work or the joined registration worker.
		// Retirement waits for both before joining these observer workers.
		g.observers.Add(1)
		s.reactors.mu.Unlock()
		stop := context.AfterFunc(ctx, cancel)
		stream, err := observerruntime.Open(runCtx, g.transport, g.id, s.name, s.namespace, plan, reactorStoreRuntime{s})
		stop()
		run.err = err
		close(run.ready)
		if err != nil {
			cancel()
			close(run.done)
			g.observers.Done()
			g.cancel()
			return err
		}
		go func() {
			defer g.observers.Done()
			defer close(run.done)
			defer cancel()
			if err := stream.Run(runCtx); err != nil && runCtx.Err() == nil {
				slog.WarnContext(runCtx, "reactor stream ended; replacing generation", "reactor", plan.Identifier(), "error", err)
				g.cancel()
			}
		}()
	}
	return nil
}

// UnregisterReactor disconnects and joins this store/namespace's reactor. Unknown
// IDs are ignored. It retains the removal across reconnect; it does not remove
// persisted kernel state. A context error means cleanup is still joining. Do not
// synchronously unregister this reactor from its own callback.
func (s *EventStore) UnregisterReactor(ctx context.Context, id reactors.ID) error {
	known := false
	for _, plan := range s.reactorPlans() {
		if plan.Identifier() == id {
			known = true
			break
		}
	}
	if !known {
		return nil
	}
	s.reactors.mu.Lock()
	if s.reactors.removed == nil {
		s.reactors.removed = make(map[reactors.ID]bool)
	}
	s.reactors.removed[id] = true
	run := s.reactors.runs[id]
	s.reactors.mu.Unlock()
	if run == nil {
		return nil
	}
	run.cancel()
	select {
	case <-run.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
