// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
)

// identityReadiness is immutable invocation-local lookup information. Computing
// external subscriptions may acquire Client.mu, so build it outside that lock.
type identityReadiness struct {
	namespace                             string
	definitionKeys                        [][2]string
	external                              string
	seed                                  string
	reactors, reducers, readModelReactors []string
}

func (s *EventStore) identityReadiness(root *definitionRoot) identityReadiness {
	p := identityReadiness{namespace: registrationKey(s.name, s.namespace, root.revision)}
	if len(root.snapshot.models.Descriptors()) > 0 {
		p.definitionKeys = append(p.definitionKeys, [2]string{definitionStageKey(s.name, root.revision, "read-models", false), definitionStageKey(s.name, root.revision, "read-models", true)})
	}
	if len(root.snapshot.projections) > 0 {
		p.definitionKeys = append(p.definitionKeys, [2]string{definitionStageKey(s.name, root.revision, "projections", false), definitionStageKey(s.name, root.revision, "projections", true)})
	}
	if len(s.externalSubscriptions()) > 0 {
		p.external = fmt.Sprintf("external-subscriptions:%q", s.name)
	}
	if !s.seedDefinition().IsEmpty() {
		p.seed = s.seedingKey()
	}
	for _, plan := range s.reactorPlans() {
		p.reactors = append(p.reactors, string(plan.Identifier()))
	}
	for _, plan := range s.reducerPlans() {
		p.reducers = append(p.reducers, string(plan.Identifier()))
	}
	for _, plan := range s.readModelReactorPlans() {
		p.readModelReactors = append(p.readModelReactors, string(plan.Identifier()))
	}
	return p
}

// identityReadyLocked never starts or joins a producer and never traverses its
// failures. Client.mu precedes observer mutexes; observer workers do not acquire
// Client.mu while holding their state mutex. No borrowed callback runs here.
func (s *EventStore) identityReadyLocked(ctx context.Context, g *generation, root *definitionRoot, p identityReadiness, ownFlight bool) identityFailure {
	c := s.client
	if c.closed {
		return identityFailure{reason: "closed", category: ErrClosed}
	}
	if err := ctx.Err(); err != nil {
		return identityContextFailure(err)
	}
	if c.current != g || g == nil || g.ctx.Err() != nil || s.definitions.root != root {
		return identityFailure{reason: "registration_not_ready"}
	}
	if s.definitions.flight && !ownFlight {
		return identityFailure{reason: "registration_not_ready"}
	}
	success := func(key string) bool {
		o := g.registrations.For(key).Snapshot()
		return o.Generation == g.number && o.IsSuccess() && !o.RetryPending
	}
	if !success(p.namespace) {
		return identityFailure{reason: "registration_not_ready"}
	}
	for _, keys := range p.definitionKeys {
		if !success(keys[0]) && !success(keys[1]) {
			return identityFailure{reason: "registration_not_ready"}
		}
	}
	if (p.external != "" && !success(p.external)) || (p.seed != "" && !success(p.seed)) ||
		!identityObserversReady(&s.reactors, g.number, p.reactors) || !identityObserversReady(&s.reducers, g.number, p.reducers) || !identityObserversReady(&s.readModelReactors, g.number, p.readModelReactors) {
		return identityFailure{reason: "registration_not_ready"}
	}
	return identityFailure{}
}

func identityObserversReady(observers *storeObservers, generation uint64, ids []string) bool {
	observers.mu.Lock()
	defer observers.mu.Unlock()
	for _, id := range ids {
		if observers.removed[id] {
			continue
		}
		run := observers.runs[id]
		if run == nil || run.generation != generation || run.openError != nil {
			return false
		}
		select {
		case <-run.ready:
		default:
			return false
		}
	}
	return true
}
