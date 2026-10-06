// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cratis/chronicle.go/internal/kernelcapability"
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
		g, attemptCtx, done, err := s.client.acquireRegistrationReady(ctx)
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

// Ready waits for a healthy generation and registrations for cached handles
// captured when called. A pass captured before EvictEventStores may still finish;
// later passes omit detached handles, even if they remain in use. Connect alone
// does not imply artifact readiness. Concurrently created stores carry their own
// barrier. Terminal failures are not retried here.
func (c *Client) Ready(ctx context.Context) error {
	if err := c.requirePrepared("ready", false); err != nil {
		return err
	}
	stores := c.storeSnapshot()
	for {
		g, attemptCtx, done, err := c.acquireRegistrationReady(ctx)
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
	return err != nil && ctx.Err() == nil && !errors.Is(err, ErrNotPrepared) && !errors.Is(err, ErrPreparationInProgress) && !terminalConnectionError(err)
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

// Explicit registration is caller-owned preparation until final raw admission.
// In particular an application token callback must not retain its own RPC join
// lease while calling Close. Destructive dispatch acquires its short lease only
// after authorization; cancellation still follows the selected generation.
func (c *Client) acquireRegistrationReady(ctx context.Context) (*generation, context.Context, func(), error) {
	g, _, release, err := c.acquireReady(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	release()
	attemptCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.ctx, cancel)
	return g, attemptCtx, func() { stop(); cancel() }, nil
}

// storeSnapshot is registration membership, never the resource ownership set.
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

func registrationKey(name StoreName, namespace Namespace, revision uint64) string {
	return fmt.Sprintf("namespace:%q:%q:%d", name, namespace, revision)
}

func (s *EventStore) register(ctx context.Context, g *generation) (RegistrationOutcome, error) {
	return s.registerWithReadiness(ctx, g, true)
}

func (s *EventStore) registerWithReadiness(ctx context.Context, g *generation, waitReady bool) (RegistrationOutcome, error) {
	for {
		if err := ctx.Err(); err != nil {
			return RegistrationOutcome{}, err
		}
		root := s.definitionRoot()
		outcome, err := s.registerRoot(ctx, g, waitReady, root)
		if !s.definitionCurrent(root) || errors.Is(err, errDefinitionSuperseded) {
			continue
		}
		return outcome, err
	}
}

func (s *EventStore) registerRoot(ctx context.Context, g *generation, waitReady bool, root *definitionRoot) (RegistrationOutcome, error) {
	// Registration is shared by every caller of this generation, whichever one
	// drives it; a gated caller's needs would refuse, and fail, all artifacts.
	// Stages that need a fix track it themselves (nestedProtectionAdmission).
	ctx = kernelcapability.Without(ctx)
	outcome := g.registrations.For(registrationKey(s.name, s.namespace, root.revision)).Run(ctx, g.number, s.client.config.registrationRetry, retryRegistration, func(ctx context.Context) ([]ArtifactRegistration, error) { return s.registerStages(ctx, g, root) })
	if outcome.Failure != nil {
		return outcome, &RegistrationError{Outcome: outcome}
	}
	// Seeding must follow observer registration sends even during background
	// replay. Without seeds, retain the nonblocking observer startup path.
	hasExternalSubscriptions := len(s.externalSubscriptions()) > 0
	waitReady = waitReady || !s.seedDefinition().IsEmpty() || hasExternalSubscriptions
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
	// C# registers external subscriptions after every observer and before
	// read-model reactors and seeding. Do not use the barrier transport here.
	if hasExternalSubscriptions {
		err := s.registerExternalSubscriptions(ctx, g)
		outcome.Artifacts = append(outcome.Artifacts, ArtifactRegistration{Name: "external-subscriptions", Failure: err})
		if err != nil {
			outcome.Failure, outcome.RetryPending = err, retryRegistration(err)
			return outcome, &RegistrationError{Outcome: outcome}
		}
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
			outcome := g.registrations.For(registrationKey(store.name, store.namespace, store.definitionRoot().revision)).Snapshot()
			if !outcome.HasRun || outcome.RetryPending || store.needsSeedRegistration(g) || store.needsExternalSubscriptionRegistration(g) {
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
