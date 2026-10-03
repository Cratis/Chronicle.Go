// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

func (s *Sequence) resolveScope(ctx context.Context, source events.SourceID, config appendConfig) (*sequences.ConcurrencyScope, error) {
	if config.scope != nil {
		return s.resolveLabeledScope(ctx, source, *config.scope)
	}
	scope, err := s.automaticScope(ctx, source, config.route)
	if err != nil {
		return nil, err
	}
	return s.resolveLabeledScope(ctx, source, scope)
}

func (s *Sequence) resolveLabeledScope(ctx context.Context, source events.SourceID, scope Scope) (*sequences.ConcurrencyScope, error) {
	resolved, err := s.resolveExpectation(ctx, source, scope)
	if err != nil {
		return nil, err
	}
	return scopeContract(source, resolved)
}

// resolvedUnchecked is distinct from Resolve: a resolved empty tail must never
// be read again at commit. Unlike NoCheck it retains the chosen history filter.
const resolvedUnchecked = 4

func (s *Sequence) resolveExpectation(ctx context.Context, source events.SourceID, scope Scope) (Scope, error) {
	scope = cloneScope(scope)
	if _, err := scopeContract(source, scope); err != nil {
		return Scope{}, err
	}
	if scope.Expectation.kind != 0 {
		return scope, nil
	}
	tail, exists, err := s.tail(ctx, scope.Filter)
	if err != nil {
		return Scope{}, fmt.Errorf("chronicle: resolve concurrency tail: %w", err)
	}
	if exists {
		scope.Expectation = Exact(tail)
	} else if s.concurrency.CheckFirstAppendIntoAScope {
		scope.Expectation = NoMatchingEvent()
	} else {
		scope.Expectation = Expectation{kind: resolvedUnchecked}
	}
	return scope, nil
}

func scopeContract(source events.SourceID, scope Scope) (*sequences.ConcurrencyScope, error) {
	filter := scope.Filter
	if filter.SourceID != nil && (*filter.SourceID == "" || *filter.SourceID != source) {
		return nil, fmt.Errorf("%w: source-bound scope must name its target or label", faults.ErrInvalidConfiguration)
	}
	filter.SourceType = scopeDimension(filter.SourceType)
	filter.StreamType = scopeDimension(filter.StreamType)
	filter.StreamID = scopeDimension(filter.StreamID)
	if scope.Expectation.kind == 1 && scope.Expectation.position >= events.Unavailable-2 {
		return nil, fmt.Errorf("%w: reserved exact sequence number", faults.ErrInvalidConfiguration)
	}
	result := &sequences.ConcurrencyScope{SequenceNumber: uint64(events.Unavailable), EventSourceId: filter.SourceID != nil,
		EventSourceType: stringValue(filter.SourceType), EventStreamType: stringValue(filter.StreamType), EventStreamId: stringValue(filter.StreamID)}
	for _, eventType := range filter.EventTypes {
		if eventType.ID == "" || strings.Contains(string(eventType.ID), ",") || eventType.Generation == 0 {
			return nil, fmt.Errorf("%w: invalid scope event type", faults.ErrInvalidConfiguration)
		}
		result.EventTypes = append(result.EventTypes, &sequences.EventType{Id: string(eventType.ID), Generation: uint32(eventType.Generation)})
	}
	switch scope.Expectation.kind {
	case 0:
		// The caller resolves only after the complete batch has been validated.
	case 1:
		result.SequenceNumber = uint64(scope.Expectation.position)
	case 2:
		result.ExpectsNoMatchingEvent = true
	case resolvedUnchecked:
		// Already resolved against an empty tail with first-append checks off.
	case 3:
		if filter.SourceID != nil || filter.SourceType != nil || filter.StreamType != nil || filter.StreamID != nil || len(filter.EventTypes) > 0 {
			return nil, fmt.Errorf("%w: NoCheck cannot carry narrowing", faults.ErrInvalidConfiguration)
		}
	default:
		return nil, faults.ErrInvalidConfiguration
	}
	return result, nil
}

func scopeDimension[T ~string](value *T) *T {
	if value != nil && *value == "" {
		return nil
	}
	return value
}

func stringValue[T ~string](value *T) string {
	if value == nil {
		return ""
	}
	return string(*value)
}
