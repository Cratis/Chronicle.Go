// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package registration coordinates immutable, generation-scoped registration passes.
package registration

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/cratis/chronicle.go/internal/connection"
)

// Artifact records an acknowledged registration stage, or its failure. A batch
// acknowledgement does not invent individual per-definition server verdicts.
type Artifact struct {
	// Name identifies the stage (store, namespace or event-types).
	Name string
	// Failure is nil only when the kernel acknowledged the stage.
	Failure error
}

// Outcome is a defensive snapshot of a completed registration pass.
type Outcome struct {
	// Generation identifies the connection that performed this pass.
	Generation uint64
	// Pass identifies the pass within this barrier and generation.
	Pass uint64
	// HasRun distinguishes not-yet-run from successful empty registration.
	HasRun bool
	// Attempts counts attempts made during this pass.
	Attempts int
	// Artifacts records acknowledged stages and the failed stage, if any.
	Artifacts []Artifact
	// Failure preserves the failure that stopped registration.
	Failure error
	// RetryPending reports whether the background worker will retry this failure.
	RetryPending bool
}

// IsSuccess requires a completed pass with no stage or pass failures.
func (o Outcome) IsSuccess() bool {
	if !o.HasRun || o.Failure != nil {
		return false
	}
	for _, artifact := range o.Artifacts {
		if artifact.Failure != nil {
			return false
		}
	}
	return true
}

// Policy bounds one pass and the subsequent background retry interval.
type Policy struct {
	// MaxAttempts is the positive number of attempts per pass.
	MaxAttempts int
	// InitialDelay is the positive initial backoff.
	InitialDelay time.Duration
	// MaximumDelay caps backoff and sets background retry delay.
	MaximumDelay time.Duration
	// AttemptTimeout bounds an individual attempt.
	AttemptTimeout time.Duration
}

type attempt struct {
	done    chan struct{}
	outcome Outcome
}

// Barrier is single-flight; only success is cached. Callbacks run without locks.
type Barrier struct {
	mu      sync.Mutex
	running *attempt
	outcome Outcome
}

func clone(o Outcome) Outcome        { o.Artifacts = slices.Clone(o.Artifacts); return o }
func (b *Barrier) Snapshot() Outcome { b.mu.Lock(); defer b.mu.Unlock(); return clone(b.outcome) }

func (b *Barrier) Run(ctx context.Context, generation uint64, policy Policy, retryable func(error) bool, run func(context.Context) ([]Artifact, error)) Outcome {
	b.mu.Lock()
	if b.outcome.IsSuccess() {
		o := clone(b.outcome)
		b.mu.Unlock()
		return o
	}
	if current := b.running; current != nil {
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return Outcome{Generation: generation, Failure: ctx.Err()}
		case <-current.done:
			return clone(current.outcome)
		}
	}
	current := &attempt{done: make(chan struct{})}
	b.running = current
	pass := b.outcome.Pass + 1
	b.mu.Unlock()
	outcome := Outcome{Generation: generation, Pass: pass, HasRun: true}
	for i := 1; i <= policy.MaxAttempts; i++ {
		outcome.Attempts = i
		attemptCtx, cancel := context.WithTimeout(ctx, policy.AttemptTimeout)
		outcome.Artifacts, outcome.Failure = run(attemptCtx)
		cancel()
		outcome.RetryPending = retryable(outcome.Failure)
		if outcome.Failure == nil || !outcome.RetryPending || ctx.Err() != nil || i == policy.MaxAttempts {
			break
		}
		if err := connection.Wait(ctx, connection.Backoff(i, policy.InitialDelay, policy.MaximumDelay)); err != nil {
			outcome.Failure = err
			break
		}
	}
	b.mu.Lock()
	b.outcome = clone(outcome)
	current.outcome = clone(outcome)
	b.running = nil
	close(current.done)
	b.mu.Unlock()
	return outcome
}

// Cache separates logical store and namespace barriers for one generation.
type Cache struct {
	mu       sync.Mutex
	barriers map[string]*Barrier
}

func (c *Cache) For(key string) *Barrier {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.barriers == nil {
		c.barriers = make(map[string]*Barrier)
	}
	if c.barriers[key] == nil {
		c.barriers[key] = &Barrier{}
	}
	return c.barriers[key]
}
