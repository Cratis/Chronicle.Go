// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"slices"
	"sort"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/protobuf/proto"
)

// DecisionTarget is an internal SDK composition seam, not a guard constructor.
func (s *Sequence) DecisionTarget() decision.Target {
	if s == nil || s.decisions == nil {
		return decision.Target{}
	}
	return s.decisions.DecisionTarget(s.id)
}

// WithDecisionGuard attaches SDK-only enrolled evidence without reconstructing
// or serializing staged events. Applications cannot manufacture a Guard.
func (b *PreparedBatch) WithDecisionGuard(guard *decision.Guard) (*PreparedBatch, error) {
	if b == nil || b.sequence == nil || guard == nil {
		return nil, decision.ErrInvalid
	}
	if guard.Evidence().Target != b.sequence.DecisionTarget() {
		return nil, decision.ErrTarget
	}
	guards := append(slices.Clone(b.guards), guard)
	if err := validateDecisionScopes(b.explicit, guards); err != nil {
		return nil, err
	}
	copy := *b
	copy.guards = guards
	return &copy, nil
}

func validateDecisionScopes(explicit []LabeledScope, guards []*decision.Guard) error {
	if len(guards) == 0 {
		return nil
	}
	for _, scope := range explicit {
		// Resolve is a new read at commit, not a checked earlier expectation.
		if scope.Scope.Expectation.kind != 1 && scope.Scope.Expectation.kind != 2 {
			return decision.ErrScope
		}
		for _, guard := range guards {
			if scope.Label == guard.Evidence().Key {
				return decision.ErrScope
			}
		}
	}
	return nil
}

// appendDecisionBatch consumes attribution resolved at the public append
// boundary. It must never infer it again from a lease or callback context.
func (s *Sequence) appendDecisionBatch(ctx context.Context, snapshot *PreparedBatch, resolvedOrigin Origin) (BatchResult, error) {
	if s.decisions == nil {
		return BatchResult{}, decision.Unsupported()
	}
	lease, err := s.decisions.AcquireDecision(ctx)
	if err != nil {
		return BatchResult{}, err
	}
	defer lease.Release()
	ctx = lease.Context
	if err = validateDecisionScopes(snapshot.explicit, snapshot.guards); err != nil {
		return BatchResult{}, err
	}
	for _, guard := range snapshot.guards {
		if err = guard.Validate(s.DecisionTarget(), lease.Generation); err != nil {
			return BatchResult{}, err
		}
	}
	batch := snapshot.batch
	batch.request = proto.CloneOf(batch.request)
	// Protected completion has no automatic effect-source defaults. Only the
	// admitted decision scopes and explicitly checked independent scopes are sent.
	for _, scope := range append(decisionScopes(snapshot.guards), snapshot.explicit...) {
		contract, err := scopeContract(events.SourceID(scope.Label), scope.Scope)
		if err != nil {
			return BatchResult{}, err
		}
		batch.request.ConcurrencyScopes = append(batch.request.ConcurrencyScopes, &sequences.EventSourceConcurrencyScope{EventSourceId: scope.Label, Scope: contract})
	}
	ctx = metadata.WithCorrelation(ctx, wire.Correlation(batch.request.CorrelationId))
	// Recheck immediately before dispatch, after all batch preparation.
	for _, guard := range snapshot.guards {
		if err = guard.Validate(s.DecisionTarget(), lease.Generation); err != nil {
			return BatchResult{}, err
		}
	}
	if err = ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	ctx = decision.WithDispatchValidation(ctx, func() error {
		for _, guard := range snapshot.guards {
			if err := guard.Validate(s.DecisionTarget(), lease.Generation); err != nil {
				return err
			}
		}
		return nil
	})
	response, rpcErr := sendBatch(ctx, batch, sequences.NewEventSequencesClient(lease.Conn))
	lease.Release() // Never retain a generation while running message/notification callbacks.
	if rpcErr == nil && lease.Resolve != nil {
		lease.Resolve(response)
	}
	return s.finishBatch(resolvedOrigin, batch, response, rpcErr)
}

func decisionScopes(guards []*decision.Guard) []LabeledScope {
	byKey := make(map[string]decision.Evidence)
	for _, guard := range guards {
		e := guard.Evidence()
		if prior, ok := byKey[e.Key]; ok {
			if prior.Boundary == events.Unavailable || (e.Boundary != events.Unavailable && prior.Boundary < e.Boundary) {
				e.Boundary = prior.Boundary
			}
			e.Types = append(e.Types, prior.Types...)
		}
		byKey[e.Key] = e
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]LabeledScope, 0, len(keys))
	for _, key := range keys {
		e := byKey[key]
		source := events.SourceID(key)
		expectation := Exact(e.Boundary)
		if e.Boundary == events.Unavailable {
			expectation = NoMatchingEvent()
		}
		sort.Slice(e.Types, func(i, j int) bool { return e.Types[i].ID < e.Types[j].ID })
		types := slices.CompactFunc(e.Types, func(a, b events.TypeRef) bool { return a.ID == b.ID })
		result = append(result, LabeledScope{Label: key, Scope: Scope{Expectation: expectation, Filter: ScopeFilter{SourceID: &source, EventTypes: types}}})
	}
	return result
}
