// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package registration coordinates immutable, generation-scoped registration passes.
package registration

import (
	"context"
	"errors"
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
	done        chan struct{}
	outcome     Outcome
	callerError error
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
	for {
		b.mu.Lock()
		if b.outcome.IsSuccess() {
			o := clone(b.outcome)
			b.mu.Unlock()
			return o
		}
		if current := b.running; current != nil {
			b.mu.Unlock()
			outcome := waitForAttempt(ctx, generation, current)
			if retryCanceledStarter(ctx, current, outcome) {
				continue
			}
			return outcome
		}
		break // Keep the lock while claiming the next pass.
	}
	current := &attempt{done: make(chan struct{})}
	b.running = current
	pass := b.outcome.Pass + 1
	b.mu.Unlock()
	return b.execute(ctx, current, generation, pass, policy, retryable, run)
}

// RunShared joins or starts a pass whose context and goroutine belong to start,
// not to a waiter. start must either schedule work with its owner's context or
// return an admission error, and the owner must cancel and join scheduled work.
func (b *Barrier) RunShared(ctx context.Context, generation uint64, policy Policy, retryable func(error) bool, run func(context.Context) ([]Artifact, error), start func(func(context.Context)) error) Outcome {
	for {
		b.mu.Lock()
		if b.outcome.IsSuccess() {
			outcome := clone(b.outcome)
			b.mu.Unlock()
			return outcome
		}
		current := b.running
		if current == nil {
			current = &attempt{done: make(chan struct{})}
			b.running = current
			pass := b.outcome.Pass + 1
			b.mu.Unlock()
			err := start(func(ownedCtx context.Context) {
				b.execute(ownedCtx, current, generation, pass, policy, retryable, run)
			})
			if err != nil {
				b.complete(current, Outcome{Generation: generation, Pass: pass, Failure: err}, nil)
			}
		} else {
			b.mu.Unlock()
		}
		outcome := waitForAttempt(ctx, generation, current)
		// An explicit runtime mutation can have started the same root's pass
		// with Run. Its caller cancellation must not fail a live readiness wait.
		if retryCanceledStarter(ctx, current, outcome) {
			continue
		}
		return outcome
	}
}

func waitForAttempt(ctx context.Context, generation uint64, current *attempt) Outcome {
	select {
	case <-ctx.Done():
		return Outcome{Generation: generation, Failure: ctx.Err()}
	case <-current.done:
		return clone(current.outcome)
	}
}

func retryCanceledStarter(ctx context.Context, current *attempt, outcome Outcome) bool {
	// Read completed attempt fields only after the completion signal. A canceled
	// waiter can leave while the starter is still publishing its result.
	if ctx.Err() != nil {
		return false
	}
	return current.callerError != nil && errors.Is(outcome.Failure, current.callerError)
}

func (b *Barrier) execute(ctx context.Context, current *attempt, generation, pass uint64, policy Policy, retryable func(error) bool, run func(context.Context) ([]Artifact, error)) Outcome {
	outcome := Outcome{Generation: generation, Pass: pass, HasRun: true}
	for i := 1; i <= policy.MaxAttempts; i++ {
		outcome.Attempts = i
		attemptCtx, cancel := context.WithTimeout(ctx, policy.AttemptTimeout)
		outcome.Artifacts, outcome.Failure = run(attemptCtx)
		cancel()
		outcome.RetryPending = retryable(outcome.Failure)
		if !continueAttempt(ctx, outcome, policy) {
			break
		}
		if err := connection.Wait(ctx, connection.Backoff(i, policy.InitialDelay, policy.MaximumDelay)); err != nil {
			outcome.Failure = err
			break
		}
	}
	b.complete(current, outcome, ctx.Err())
	return outcome
}

func continueAttempt(ctx context.Context, outcome Outcome, policy Policy) bool {
	return outcome.Failure != nil && outcome.RetryPending && ctx.Err() == nil && outcome.Attempts < policy.MaxAttempts
}

func (b *Barrier) complete(current *attempt, outcome Outcome, callerError error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.outcome = clone(outcome)
	current.outcome = clone(outcome)
	current.callerError = callerError
	b.running = nil
	close(current.done)
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
