// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"

	seedcontracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/internal/registration"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/seeding"
)

func (s *EventStore) seedDefinition() seeding.Definition {
	if seeds, ok := s.client.storeSeeds[s.name]; ok {
		return seeds
	}
	return s.client.seeds
}

func (s *EventStore) seedingKey() string { return fmt.Sprintf("seeding:%q", s.name) }

func (s *EventStore) needsSeedRegistration(g *generation) bool {
	if s.seedDefinition().IsEmpty() {
		return false
	}
	outcome := g.registrations.For(s.seedingKey()).Snapshot()
	return !outcome.HasRun || outcome.RetryPending
}

func (s *EventStore) registerSeeds(ctx context.Context, g *generation) RegistrationOutcome {
	return g.registrations.For(s.seedingKey()).Run(ctx, g.number, s.client.config.registrationRetry, retryRegistration, func(ctx context.Context) ([]ArtifactRegistration, error) {
		err := s.sendSeeds(ctx, g, s.seedDefinition())
		return []ArtifactRegistration{{Name: "seeding", Failure: err}}, err
	})
}

func (s *EventStore) sendSeeds(ctx context.Context, g *generation, definition seeding.Definition) error {
	if definition.IsEmpty() {
		return ctx.Err()
	}
	result, err := seedcontracts.NewEventSeedingClient(g.transport).SeedEvents(ctx, definition.Contract(s.name))
	if err != nil {
		return err
	}
	return wire.CheckEnvelope(result)
}

// SeedBatch is a manually prepared, immutable batch bound to one store. It is
// safe for concurrent Register calls. Failure retains the batch; success consumes
// pending work. The client owns the connection; a batch owns no transport resources.
type SeedBatch struct {
	store        *EventStore
	definition   seeding.Definition
	registration registration.Barrier
}

// PrepareSeeds invokes a seeder synchronously against this store's frozen event
// catalog, without I/O. The returned batch retains serialized entries for retry;
// callbacks never run during Register. Use a new batch for corrected definitions.
// Like registered seeders, unscoped entries are global, not handle-local.
func (s *EventStore) PrepareSeeds(seeder seeding.Seeder) (*SeedBatch, error) {
	definition, err := seeding.Prepare(s.catalog, seeder)
	if err != nil {
		return nil, err
	}
	return &SeedBatch{store: s, definition: definition}, nil
}

// Register waits for observers and sends through the kernel's idempotent seeding
// protocol. Calls share a single flight; after success later calls are no-ops.
// Transient failures use the client's bounded registration retry policy; a later
// call retries retained work after any failure. No append RPC is retried here.
func (b *SeedBatch) Register(ctx context.Context) error {
	if b == nil || b.store == nil {
		return fmt.Errorf("%w: unprepared seed batch", ErrInvalidConfiguration)
	}
	if b.registration.Snapshot().IsSuccess() {
		return ctx.Err()
	}
	for {
		g, attemptCtx, done, err := b.store.client.acquireReady(ctx)
		if err != nil {
			return err
		}
		if _, err = b.store.register(attemptCtx, g); err == nil {
			outcome := b.registration.Run(attemptCtx, g.number, b.store.client.config.registrationRetry, retryRegistration, func(ctx context.Context) ([]ArtifactRegistration, error) {
				err := b.store.sendSeeds(ctx, g, b.definition)
				return []ArtifactRegistration{{Name: "seeding", Failure: err}}, err
			})
			if outcome.Failure != nil {
				err = &RegistrationError{Outcome: outcome}
			}
		}
		retry := g.ctx.Err() != nil && retryReadiness(ctx, err)
		done()
		if !retry {
			return err
		}
	}
}
