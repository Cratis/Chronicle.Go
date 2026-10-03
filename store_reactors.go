// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

type storeReactors struct {
	mu      sync.Mutex
	runs    map[reactors.ID]*reactorRun
	retired map[reactors.ID]map[*reactorRun]struct{}
	removed map[reactors.ID]bool
}
type reactorRun struct {
	generation uint64
	cancel     context.CancelFunc
	ready      chan struct{}
	openError  error // Protected by storeReactors.mu; cleared on a successful Open.
	done       chan struct{}
}

func (s *EventStore) reactorPlans() []*reactors.Plan {
	if plans, ok := s.client.reactors.stores[s.name]; ok {
		return plans
	}
	return s.client.reactors.defaults
}
func (s *EventStore) startReactors(ctx context.Context, g *generation, waitReady bool) error {
	for _, plan := range s.reactorPlans() {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.reactors.mu.Lock()
		if s.reactors.removed[plan.Identifier()] {
			s.reactors.mu.Unlock()
			continue
		}
		run := s.reactors.runs[plan.Identifier()]
		if run == nil || run.generation != g.number {
			if run != nil {
				select {
				case <-run.done:
				default:
					if s.reactors.retired == nil {
						s.reactors.retired = make(map[reactors.ID]map[*reactorRun]struct{})
					}
					if s.reactors.retired[plan.Identifier()] == nil {
						s.reactors.retired[plan.Identifier()] = make(map[*reactorRun]struct{})
					}
					s.reactors.retired[plan.Identifier()][run] = struct{}{}
				}
			}
			runCtx, cancel := context.WithCancel(g.ctx)
			run = &reactorRun{generation: g.number, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}
			if s.reactors.runs == nil {
				s.reactors.runs = make(map[reactors.ID]*reactorRun)
			}
			s.reactors.runs[plan.Identifier()] = run
			// Admission is pinned by g.work or the registration worker. Retirement
			// drains both before joining observers, so Add cannot race with Wait.
			g.observers.Add(1)
			go s.runReactor(runCtx, g, plan, run)
		}
		s.reactors.mu.Unlock()
		if !waitReady {
			continue
		}
		// A caller owns only its readiness wait, never the subscription lifetime.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.ctx.Done():
			return g.ctx.Err()
		case <-run.ready:
		}
		s.reactors.mu.Lock()
		err := run.openError
		removed := s.reactors.removed[plan.Identifier()]
		s.reactors.mu.Unlock()
		if err != nil && !removed {
			return fmt.Errorf("chronicle: reactor %q subscription: %w", plan.Identifier(), err)
		}
	}
	return nil
}

func (s *EventStore) runReactor(ctx context.Context, g *generation, plan *reactors.Plan, run *reactorRun) {
	defer g.observers.Done()
	defer func() {
		s.reactors.mu.Lock()
		close(run.done)
		delete(s.reactors.retired[plan.Identifier()], run)
		if len(s.reactors.retired[plan.Identifier()]) == 0 {
			delete(s.reactors.retired, plan.Identifier())
		}
		s.reactors.mu.Unlock()
	}()
	defer run.cancel()
	var ready sync.Once
	markReady := func(err error) {
		s.reactors.mu.Lock()
		run.openError = err
		ready.Do(func() { close(run.ready) })
		s.reactors.mu.Unlock()
	}
	// Removal during Open releases readiness waiters without poisoning registration.
	defer markReady(nil)
	for ctx.Err() == nil {
		// Each failed attempt releases its stream before another is opened.
		attempt, cancel := context.WithCancel(ctx)
		stream, err := observerruntime.Open(attempt, g.transport, g.id, s.name, s.namespace, plan, reactorStoreRuntime{s})
		if ctx.Err() == nil {
			// Report the first Open outcome without stopping independent retries.
			markReady(err)
		}
		if err == nil {
			err = stream.Run(attempt)
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		slog.WarnContext(ctx, "reactor stream ended; resubscribing", "reactor", plan.Identifier(), "error", err)
		if s.client.config.reactorRetryWait(ctx, 2*time.Second) != nil {
			return
		}
	}
}

// UnregisterReactor disconnects and joins this store/namespace's reactor across
// current and retired generations. Unknown IDs are ignored. It retains the
// removal across reconnect; it does not remove persisted kernel state. Reconnect
// may overlap old and new generation delivery until old callbacks return, as in
// C#. A context error means cleanup is still joining. Do not synchronously
// unregister this reactor from its own callback.
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
	var runs []*reactorRun
	if run := s.reactors.runs[id]; run != nil {
		runs = append(runs, run)
	}
	for run := range s.reactors.retired[id] {
		runs = append(runs, run)
	}
	s.reactors.mu.Unlock()
	for _, run := range runs {
		run.cancel()
	}
	for _, run := range runs {
		select {
		case <-run.done:
		case <-ctx.Done():
			return ctx.Err()
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
