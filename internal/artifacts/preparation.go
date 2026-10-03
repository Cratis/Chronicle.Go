// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync/atomic"
	"time"
)

// PreparationError describes a failed preparation boundary without formatting
// application errors or panic values. Ordinary causes remain available through
// Unwrap; recovered panic values are discarded, never retained as diagnostics.
type PreparationError struct {
	// Family identifies the definition family, or registry-wide preparation.
	Family string
	// Stage identifies the failed preparation operation, not application data.
	Stage string
	cause error
}

// Error returns payload-free preparation diagnostics.
func (e *PreparationError) Error() string {
	return "chronicle: " + e.Family + " preparation failed during " + e.Stage
}

// Unwrap preserves application and cleanup failure identities.
func (e *PreparationError) Unwrap() error { return e.cause }

// Format redacts application causes for every fmt verb.
func (e *PreparationError) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, e.Error()) }

type panicError struct{}

func (*panicError) Error() string                { return "application callback panicked" }
func (*panicError) Unwrap() error                { return invalid("application callback panicked") }
func (e *panicError) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, e.Error()) }

// Protect redacts errors and recovers panics at a synchronous preparation boundary.
func Protect(family, stage string, run func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = &panicError{}
		}
		if err != nil {
			if _, prepared := err.(*PreparationError); !prepared {
				err = &PreparationError{Family: family, Stage: stage, cause: err}
			}
		}
	}()
	return run()
}

// ValidateDependencies checks visible constructor dependencies without activation.
func (c Constructor) ValidateDependencies(check func(reflect.Type) error) error {
	for _, typ := range c.args {
		if err := check(typ); err != nil {
			return err
		}
	}
	return nil
}

// Prepare opens, constructs, prepares immutable output, then closes before
// reporting success. Provider-resolved services are borrowed; constructor results
// (including nonnil partial results) are owned and closed before the scope. No
// callback executes under an internal lock or a client/generation work lease.
// Cleanup preserves metadata but ignores cancellation, with a cooperative 5s bound.
func Prepare(ctx context.Context, family string, services ScopeFactory, constructor Constructor, check func(reflect.Type) error, prepare func(any) error) (err error) {
	var lease *Lease
	guard := &preparationScope{check: check}
	guard.active.Store(true)
	defer func() {
		guard.active.Store(false)
		if lease != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			closeErr := Protect(family, "close", func() error { return lease.Close(cleanup) })
			if closeErr != nil {
				err = &PreparationError{Family: family, Stage: "close", cause: errors.Join(err, closeErr)}
			}
		}
	}()
	if err = Protect(family, "open", func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var openErr error
		lease, openErr = Open(ctx, services)
		return openErr
	}); err != nil {
		return err
	}
	guard.scope = lease.Scope
	var value any
	if err = Protect(family, "construct", func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var constructErr error
		value, constructErr = lease.construct(ctx, constructor, guard)
		return constructErr
	}); err != nil {
		return err
	}
	return Protect(family, "define", func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := prepare(value); err != nil {
			return err
		}
		return ctx.Err()
	})
}

type preparationScope struct {
	scope  Scope
	check  func(reflect.Type) error
	active atomic.Bool
}

func (s *preparationScope) Resolve(ctx context.Context, typ reflect.Type) (any, error) {
	if !s.active.Load() {
		return nil, invalid("preparation resolver expired")
	}
	if typ == scopeType {
		return s, nil
	}
	if s.check != nil {
		if err := s.check(typ); err != nil {
			return nil, err
		}
	}
	value, err := Resolve(ctx, s.scope, typ)
	if !s.active.Load() {
		return nil, invalid("preparation resolver expired")
	}
	return value, err
}
func (*preparationScope) Close(context.Context) error {
	return invalid("preparation resolver is borrowed")
}
