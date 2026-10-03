// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

// ConcurrencyScopeStrategy replaces automatic source-and-route scope selection
// and is also used by ResolveScope for explicitly selected filters.
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

// ResolveScope applies the configured custom or default concurrency strategy to
// filter and resolves any returned Resolve expectation using its matching tail.
// It honors CheckFirstAppendIntoAScope: an empty tail becomes NoMatchingEvent when
// enabled, otherwise a resolved unchecked expectation retaining the filter.
// Explicit strategy expectations win. The returned scope can be staged or passed
// to WithScope/WithScopes without another strategy invocation or tail read.
//
// Supply normalized dimensions (nil means no narrowing); no append-route defaults
// are added. Empty optional dimensions are non-narrowing. Inputs and returned
// filter collections are copied; do not mutate inputs during the call. The method
// is safe for concurrent use, honors ctx, never appends and never retries. It does
// not protect an earlier history read; use ReadHistory for that. Invalid filters
// or strategy scopes return ErrInvalidConfiguration; strategy/RPC errors retain
// their identities. The Sequence must have been constructed with New.
func (s *Sequence) ResolveScope(ctx context.Context, filter ScopeFilter) (Scope, error) {
	if err := ctx.Err(); err != nil {
		return Scope{}, err
	}
	if s == nil || s.service == nil {
		return Scope{}, faults.ErrInvalidConfiguration
	}
	scope := cloneScope(Scope{Filter: filter})
	scope.Filter.SourceType = scopeDimension(scope.Filter.SourceType)
	scope.Filter.StreamType = scopeDimension(scope.Filter.StreamType)
	scope.Filter.StreamID = scopeDimension(scope.Filter.StreamID)
	source := events.SourceID(stringValue(scope.Filter.SourceID))
	if scope.Filter.SourceID != nil && strings.TrimSpace(string(source)) == "" {
		return Scope{}, faults.ErrInvalidConfiguration
	}
	if _, err := scopeContract(source, scope); err != nil {
		return Scope{}, err
	}
	if s.concurrency.Strategy != nil {
		var err error
		scope, err = s.concurrency.Strategy.GetScope(ctx, s, scope.Filter)
		if err != nil {
			return Scope{}, err
		}
	}
	return s.resolveExpectation(ctx, source, scope)
}

func (s *Sequence) automaticScope(ctx context.Context, source events.SourceID, route Route) (Scope, error) {
	return s.ResolveScope(ctx, defaultScope(source, route).Filter)
}

func (s *Sequence) automaticBatchScopes(ctx context.Context, entries []Entry, explicit []LabeledScope) ([]LabeledScope, error) {
	scopes, err := batchScopes(entries, explicit)
	if err != nil {
		return nil, err
	}
	for i := len(explicit); i < len(scopes); i++ {
		scope, err := s.ResolveScope(ctx, scopes[i].Scope.Filter)
		if err != nil {
			return nil, err
		}
		scopes[i].Scope = scope
	}
	return scopes, nil
}
