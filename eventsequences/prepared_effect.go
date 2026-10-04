// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/reactoreffects"
	"google.golang.org/protobuf/proto"
)

// PrepareReturnedEvents is the module-private reactor seam. It freezes all local
// content/audit/origin work before any built-in append or custom effect executes.
// Dispatch preserves the original single, legacy-many or heterogeneous route.
func (s *Sequence) PrepareReturnedEvents(ctx context.Context, effect reactoreffects.Preparation[Entry, LabeledScope]) (func(context.Context) error, error) {
	if len(effect.Entries) == 0 && !effect.Batch {
		return func(context.Context) error { return nil }, nil
	}
	if len(effect.Entries) == 0 && !hasPotentialReturnedCheck(effect.Scopes) {
		return nil, faults.ErrInvalidConfiguration
	}
	config := batchConfig{scopes: effect.Scopes}
	if effect.Bare && !effect.Single {
		for _, entry := range effect.Entries {
			descriptor, ok := s.catalog.Lookup(entry.Event)
			if ok {
				config.tags = append(config.tags, descriptor.Tags()...)
			}
		}
	}
	batch, err := s.snapshotBatch(ctx, effect.Entries, config)
	if err != nil {
		return nil, err
	}
	origin, err := s.resolveAppendOrigin(ctx)
	if err != nil {
		return nil, err
	}
	// Keep coordinates only, never retain the event objects or metadata aliases.
	coordinates := make([]Entry, len(effect.Entries))
	for i, entry := range effect.Entries {
		coordinates[i] = Entry{Source: entry.Source, Route: entry.Route}
	}
	scopes := make([]LabeledScope, len(effect.Scopes))
	for i, scope := range effect.Scopes {
		scopes[i] = LabeledScope{Label: scope.Label, Scope: cloneScope(scope.Scope)}
	}
	bare, single := effect.Bare, effect.Single
	return func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		ctx = batch.audit.Context(ctx)
		var err error
		pending := batch
		pending.request = proto.CloneOf(batch.request)
		if bare && single {
			first := coordinates[0]
			request := singleRequest(pending.request, pending.request.Events[0])
			request.ConcurrencyScope, err = s.resolveScope(ctx, first.Source, appendConfig{route: first.Route})
			if err != nil {
				return err
			}
			result, err := s.dispatchSingle(ctx, request, batch.refs[0], first.Route, nil, origin)
			if err != nil {
				return err
			}
			return result.Err()
		}
		resolved, err := s.automaticBatchScopes(ctx, coordinates, scopes)
		if err != nil {
			return err
		}
		pending, err = s.resolveBatch(ctx, pending, resolved)
		if err != nil {
			return err
		}
		var result BatchResult
		if bare && normalizedRoute(coordinates[0].Route) == normalizedRoute(Route{}) {
			result, err = s.dispatchMany(ctx, coordinates[0].Source, appendConfig{}, pending, origin)
		} else {
			result, err = s.dispatchBatch(ctx, pending, origin)
		}
		if err != nil {
			return err
		}
		return result.Err()
	}, nil
}

// Resolve may still acquire protection from a matching tail. Only empty
// effects with no potentially checked scope are statically impossible.
func hasPotentialReturnedCheck(scopes []LabeledScope) bool {
	for _, scope := range scopes {
		if scope.Scope.Expectation.kind == 0 || scope.Scope.Expectation.kind == 1 || scope.Scope.Expectation.kind == 2 {
			return true
		}
	}
	return false
}

// Assert the private contract without growing the public EventAppender interface.
var _ reactoreffects.Preparer[Entry, LabeledScope] = (*Sequence)(nil)
