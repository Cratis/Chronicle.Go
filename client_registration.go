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
func (s *EventStore) WaitForRegistration(ctx context.Context) (RegistrationOutcome, error) {
	if err := s.client.connect(ctx, false); err != nil {
		return RegistrationOutcome{}, err
	}
	g, ctx, done, err := s.client.acquire(ctx)
	if err != nil {
		return RegistrationOutcome{}, err
	}
	defer done()
	outcome, err := s.register(ctx, g)
	if err == nil && ctx.Err() != nil {
		return outcome, ctx.Err()
	}
	return outcome, err
}

// Ready waits for a healthy generation and registrations for all handles known
// when called. Connect alone does not imply artifact readiness. Concurrently
// created stores carry their own barrier. Terminal failures are not retried here.
func (c *Client) Ready(ctx context.Context) error {
	if err := c.connect(ctx, false); err != nil {
		return err
	}
	g, ctx, done, err := c.acquire(ctx)
	if err != nil {
		return err
	}
	defer done()
	for _, store := range c.storeSnapshot() {
		if _, err = store.register(ctx, g); err != nil {
			return err
		}
	}
	return ctx.Err()
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
	outcome := g.registrations.For(registrationKey(s.name, s.namespace)).Run(ctx, g.number, s.client.config.registrationRetry, retryRegistration, func(ctx context.Context) ([]ArtifactRegistration, error) { return s.registerStages(ctx, g) })
	if outcome.Failure != nil {
		return outcome, &RegistrationError{Outcome: outcome}
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
			if !outcome.HasRun || outcome.RetryPending {
				// This worker owns retrying; a failed pass stays visible to readiness callers.
				_, _ = store.register(g.ctx, g)
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
