// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cratis/chronicle.go/internal/registration"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegistrationOutcome records a generation-scoped pass, acknowledged stages,
// failures, attempts and whether a delayed retry is pending. Zero is not success.
type RegistrationOutcome = registration.Outcome

// ArtifactRegistration records a registration-stage acknowledgement or failure.
type ArtifactRegistration = registration.Artifact

// RegistrationError preserves both partial progress and the inspectable cause.
type RegistrationError struct {
	// Outcome is the completed pass; modifying its slices does not affect the client.
	Outcome RegistrationOutcome
}

func (e *RegistrationError) Error() string {
	return fmt.Sprintf("chronicle: registration failed: %v", e.Outcome.Failure)
}

// Unwrap preserves context, envelope and transport error inspection.
func (e *RegistrationError) Unwrap() error { return e.Outcome.Failure }

// WaitForRegistration connects if necessary and joins or starts a required pass
// for this namespace and current generation. Failures are explicit and retryable
// on a later call; successful store-wide definitions are shared across namespaces.
// A failed reactor Open is reported with its ID while subscription retries continue;
// a later call observes successful readiness once Open recovers.
func (s *EventStore) WaitForRegistration(ctx context.Context) (RegistrationOutcome, error) {
	for {
		g, attemptCtx, done, err := s.client.acquireReady(ctx)
		if err != nil {
			return RegistrationOutcome{}, err
		}
		outcome, err := s.register(attemptCtx, g)
		if err == nil {
			err = g.ctx.Err()
		}
		if err == nil {
			err = attemptCtx.Err()
		}
		retry := g.ctx.Err() != nil && retryReadiness(ctx, err)
		done()
		if !retry {
			return outcome, err
		}
	}
}

// Ready waits for a healthy generation and registrations for all handles known
// when called. Connect alone does not imply artifact readiness. Concurrently
// created stores carry their own barrier. Terminal failures are not retried here.
func (c *Client) Ready(ctx context.Context) error {
	stores := c.storeSnapshot()
	for {
		g, attemptCtx, done, err := c.acquireReady(ctx)
		if err != nil {
			return err
		}
		for _, store := range stores {
			if _, err = store.register(attemptCtx, g); err != nil {
				break
			}
		}
		if err == nil {
			err = g.ctx.Err()
		}
		if err == nil {
			err = attemptCtx.Err()
		}
		retry := g.ctx.Err() != nil && retryReadiness(ctx, err)
		done()
		if !retry {
			return err
		}
	}
}

func retryReadiness(ctx context.Context, err error) bool {
	return err != nil && ctx.Err() == nil && !terminalConnectionError(err)
}

// acquireReady tolerates a generation disappearing between connection readiness
// and admission. It never retries a dispatched application operation.
func (c *Client) acquireReady(ctx context.Context) (*generation, context.Context, func(), error) {
	for {
		if err := c.connect(ctx, false); err != nil {
			if retryReadiness(ctx, err) {
				continue
			}
			return nil, nil, nil, err
		}
		g, attemptCtx, done, err := c.acquire(ctx)
		if !retryReadiness(ctx, err) {
			return g, attemptCtx, done, err
		}
	}
}

func (c *Client) storeSnapshot() []*EventStore {
	c.mu.Lock()
	stores := make([]*EventStore, 0, len(c.stores))
	for _, store := range c.stores {
		stores = append(stores, store)
	}
	c.mu.Unlock()
	sort.Slice(stores, func(i, j int) bool {
		if stores[i].name != stores[j].name {
			return stores[i].name < stores[j].name
		}
		return stores[i].namespace < stores[j].namespace
	})
	return stores
}

func retryRegistration(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}

func registrationKey(name StoreName, namespace Namespace) string {
	return fmt.Sprintf("namespace:%q:%q", name, namespace)
}

func (s *EventStore) register(ctx context.Context, g *generation) (RegistrationOutcome, error) {
	return s.registerWithReadiness(ctx, g, true)
}

func (s *EventStore) registerWithReadiness(ctx context.Context, g *generation, waitReady bool) (RegistrationOutcome, error) {
	outcome := g.registrations.For(registrationKey(s.name, s.namespace)).Run(ctx, g.number, s.client.config.registrationRetry, retryRegistration, func(ctx context.Context) ([]ArtifactRegistration, error) { return s.registerStages(ctx, g) })
	if outcome.Failure != nil {
		return outcome, &RegistrationError{Outcome: outcome}
	}
	// Seeding must follow observer registration sends even during background
	// replay. Without seeds, retain the nonblocking observer startup path.
	waitReady = waitReady || !s.seedDefinition().IsEmpty()
	if err := s.startReactors(ctx, g, waitReady); err != nil {
		outcome.Failure = err
		outcome.RetryPending = ctx.Err() == nil && g.ctx.Err() == nil
		outcome.Artifacts = append(outcome.Artifacts, ArtifactRegistration{Name: "reactors", Failure: err})
		return outcome, &RegistrationError{Outcome: outcome}
	}
	if err := s.startReducers(ctx, g, waitReady); err != nil {
		outcome.Failure = err
		outcome.RetryPending = ctx.Err() == nil && g.ctx.Err() == nil
		outcome.Artifacts = append(outcome.Artifacts, ArtifactRegistration{Name: "reducers", Failure: err})
		return outcome, &RegistrationError{Outcome: outcome}
	}
	if err := s.startReadModelReactors(ctx, g, waitReady); err != nil {
		outcome.Failure = err
		outcome.Artifacts = append(outcome.Artifacts, ArtifactRegistration{Name: "read-model-reactors", Failure: err})
		return outcome, &RegistrationError{Outcome: outcome}
	}
	if !s.seedDefinition().IsEmpty() {
		seedOutcome := s.registerSeeds(ctx, g)
		outcome.Artifacts = append(outcome.Artifacts, seedOutcome.Artifacts...)
		outcome.Attempts = max(outcome.Attempts, seedOutcome.Attempts)
		outcome.Failure, outcome.RetryPending = seedOutcome.Failure, seedOutcome.RetryPending
		if outcome.Failure != nil {
			return outcome, &RegistrationError{Outcome: outcome}
		}
	}
	return outcome, nil
}

func (c *Client) replayRegistrations(g *generation) {
	timer := time.NewTimer(c.config.registrationRetry.MaximumDelay)
	defer timer.Stop()
	stores := g.initialStores
	g.initialStores = nil
	for {
		for _, store := range stores {
			if g.ctx.Err() != nil {
				return
			}
			outcome := g.registrations.For(registrationKey(store.name, store.namespace)).Snapshot()
			if !outcome.HasRun || outcome.RetryPending || store.needsSeedRegistration(g) {
				// Ordinary replay starts observer workers without waiting. Seeded
				// stores must await their registration before dispatching seed data.
				_, _ = store.registerWithReadiness(g.ctx, g, false)
			}
		}
		select {
		case <-g.ctx.Done():
			return
		case <-timer.C:
		}
		timer.Reset(c.config.registrationRetry.MaximumDelay)
		stores = c.storeSnapshot()
	}
}
