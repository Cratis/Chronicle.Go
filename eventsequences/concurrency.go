// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"

	"github.com/cratis/chronicle.go/events"
)

// ConcurrencyScopeStrategy replaces automatic source-and-route scope selection.
// GetScope may use sequence.Tail with the caller's context, or return a Resolve
// expectation to defer the one matching tail read to dispatch. Returned scopes
// are validated like explicit scopes. Implementations must support concurrent
// calls, must honor cancellation, and must not mutate the sequence or filter.
// Explicit WithScope/WithScopes bypass this strategy.
type ConcurrencyScopeStrategy interface {
	GetScope(ctx context.Context, sequence *Sequence, filter ScopeFilter) (Scope, error)
}

// ConcurrencyPolicy configures default scope resolution. The zero value uses
// optimistic source-and-route concurrency and leaves empty tails unchecked.
// CheckFirstAppendIntoAScope protects empty Resolve expectations, including
// explicit Resolve scopes; explicit Exact, NoMatchingEvent and NoCheck win.
// Strategy is borrowed for the sequence's lifetime; nil selects the default.
type ConcurrencyPolicy struct {
	// CheckFirstAppendIntoAScope protects an empty matching tail (default false).
	CheckFirstAppendIntoAScope bool
	// Strategy replaces automatic scope selection; nil uses optimistic resolution.
	Strategy ConcurrencyScopeStrategy
}

// ConcurrencyPolicyProvider optionally supplies immutable policy on a low-level
// connection passed to New. Chronicle clients implement this automatically.
type ConcurrencyPolicyProvider interface {
	ConcurrencyPolicy() ConcurrencyPolicy
}

func (s *Sequence) automaticScope(ctx context.Context, source events.SourceID, route Route) (Scope, error) {
	scope := defaultScope(source, route)
	if s.concurrency.Strategy == nil {
		return scope, nil
	}
	return s.concurrency.Strategy.GetScope(ctx, s, cloneScope(scope).Filter)
}

func (s *Sequence) automaticBatchScopes(ctx context.Context, entries []Entry, explicit []LabeledScope) ([]LabeledScope, error) {
	scopes, err := batchScopes(entries, explicit)
	if err != nil {
		return nil, err
	}
	if s.concurrency.Strategy == nil {
		return scopes, nil
	}
	for i := len(explicit); i < len(scopes); i++ {
		scope, err := s.concurrency.Strategy.GetScope(ctx, s, cloneScope(scopes[i].Scope).Filter)
		if err != nil {
			return nil, err
		}
		scopes[i].Scope = scope
	}
	return scopes, nil
}
