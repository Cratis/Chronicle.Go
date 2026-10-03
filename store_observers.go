// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type observerStream interface{ Run(context.Context) error }
type observerPlan struct {
	id      string
	open    func(context.Context, *generation) (observerStream, error)
	oneShot bool // Read-model changes have no durable cursor: never silently resume.
}
type storeObservers struct {
	mu      sync.Mutex
	runs    map[string]*observerRun
	retired map[string]map[*observerRun]struct{}
	removed map[string]bool
}
type observerRun struct {
	generation uint64
	cancel     context.CancelFunc
	ready      chan struct{}
	openError  error // Protected by storeObservers.mu.
	done       chan struct{}
}

// start retains isolated per-observer retry workers across registration calls.
// g.work or the registration worker pins admission before retirement joins observers.
func (s *storeObservers) start(ctx context.Context, g *generation, waitReady bool, plans []observerPlan, wait func(context.Context, time.Duration) error) error {
	for _, plan := range plans {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if s.removed[plan.id] {
			s.mu.Unlock()
			continue
		}
		run := s.runs[plan.id]
		if run == nil || run.generation != g.number {
			if run != nil {
				select {
				case <-run.done:
				default:
					if s.retired == nil {
						s.retired = make(map[string]map[*observerRun]struct{})
					}
					if s.retired[plan.id] == nil {
						s.retired[plan.id] = make(map[*observerRun]struct{})
					}
					s.retired[plan.id][run] = struct{}{}
				}
			}
			runCtx, cancel := context.WithCancel(g.ctx)
			run = &observerRun{generation: g.number, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}
			if s.runs == nil {
				s.runs = make(map[string]*observerRun)
			}
			s.runs[plan.id] = run
			g.observers.Add(1)
			go s.run(runCtx, g, plan, run, wait)
		}
		s.mu.Unlock()
		if !waitReady {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.ctx.Done():
			return g.ctx.Err()
		case <-run.ready:
		}
		s.mu.Lock()
		err, removed := run.openError, s.removed[plan.id]
		s.mu.Unlock()
		if err != nil && !removed {
			return fmt.Errorf("chronicle: observer %q subscription: %w", plan.id, err)
		}
	}
	return nil
}
func (s *storeObservers) run(ctx context.Context, g *generation, plan observerPlan, run *observerRun, wait func(context.Context, time.Duration) error) {
	defer g.observers.Done()
	defer func() {
		s.mu.Lock()
		close(run.done)
		delete(s.retired[plan.id], run)
		if len(s.retired[plan.id]) == 0 {
			delete(s.retired, plan.id)
		}
		s.mu.Unlock()
	}()
	defer run.cancel()
	var ready sync.Once
	markReady := func(err error) {
		s.mu.Lock()
		run.openError = err
		ready.Do(func() { close(run.ready) })
		s.mu.Unlock()
	}
	defer ready.Do(func() { close(run.ready) })
	for ctx.Err() == nil {
		attempt, cancel := context.WithCancel(ctx)
		stream, err := plan.open(attempt, g)
		if ctx.Err() == nil {
			markReady(err)
		}
		if err == nil {
			err = stream.Run(attempt)
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		if plan.oneShot {
			// Startup errors remain on the ready barrier; after readiness a
			// best-effort watch failure must not poison unrelated reads/appends.
			// The stream reports its terminal outcome through its error handler.
			return
		}
		slog.WarnContext(ctx, "observer stream ended; resubscribing", "observer", plan.id, "error", err)
		if wait(ctx, 2*time.Second) != nil {
			return
		}
	}
}
func (s *storeObservers) unregister(ctx context.Context, id string) error {
	s.mu.Lock()
	if s.removed == nil {
		s.removed = make(map[string]bool)
	}
	s.removed[id] = true
	var runs []*observerRun
	if run := s.runs[id]; run != nil {
		runs = append(runs, run)
	}
	for run := range s.retired[id] {
		runs = append(runs, run)
	}
	s.mu.Unlock()
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
