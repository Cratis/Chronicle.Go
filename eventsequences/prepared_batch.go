// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/appendorigin"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/protobuf/proto"
)

// PreparedBatch is an immutable, serialized batch bound to its originating
// Sequence handle. Use PrepareBatch; its zero value is invalid. It owns its
// content, metadata and scopes, but not the connection. It is not a transaction:
// use transactions.Begin for once-only completion and staging ownership.
type PreparedBatch struct {
	sequence *Sequence
	batch    preparedBatch
	entries  []Entry // Only source/route, for first-entry default scopes.
	explicit []LabeledScope
	guards   []*decision.Guard
}

// PrepareBatch snapshots entries, context metadata and scopes without I/O.
// Serialization and all local validation happen now; Resolve tails are read only
// by AppendPreparedBatch. Empty snapshots are allowed for incremental enrollment.
// Inputs are borrowed until return and must not be mutated concurrently.
func (s *Sequence) PrepareBatch(ctx context.Context, entries []Entry, options ...BatchOption) (*PreparedBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !s.canPrepareBatch() {
		return nil, faults.ErrInvalidConfiguration
	}
	config := batchConfig{correlation: metadata.Correlation(ctx)}
	for _, option := range options {
		if option == nil {
			return nil, faults.ErrInvalidConfiguration
		}
		option(&config)
	}
	if _, err := batchScopes(entries, config.scopes); err != nil {
		return nil, err
	}
	batch, err := s.snapshotBatch(ctx, entries, config)
	if err != nil {
		return nil, err
	}
	freezeBatchCausation(batch.request)
	return newPreparedBatch(s, batch, entries, config.scopes), nil
}

func (s *Sequence) canPrepareBatch() bool {
	return s != nil && s.catalog != nil && s.service != nil
}

func freezeBatchCausation(request *sequences.AppendManyForEventSourcesRequest) {
	// Every stage has its own audit chain. Do not let an empty per-entry chain
	// inherit a different stage's common chain when snapshots are joined.
	for _, event := range request.Events {
		if len(event.Causation) == 0 {
			event.Causation = request.Causation
		}
	}
	request.Causation = nil
}

func newPreparedBatch(sequence *Sequence, batch preparedBatch, entries []Entry, scopes []LabeledScope) *PreparedBatch {
	result := &PreparedBatch{sequence: sequence, batch: batch}
	for _, entry := range entries {
		result.entries = append(result.entries, Entry{Source: entry.Source, Route: entry.Route, EventSource: entry.EventSource})
	}
	for _, labeled := range scopes {
		result.explicit = append(result.explicit, LabeledScope{Label: labeled.Label, Scope: cloneScope(labeled.Scope)})
	}
	return result
}

// Merge returns an ordered snapshot of b followed by next, without changing
// either input. Both must use the same Sequence handle, correlation and actor.
// Explicit scopes replace implicit defaults. Repeated identical scopes join;
// conflicting scopes fail atomically. Event-type filters compare as sets.
func (b *PreparedBatch) Merge(next *PreparedBatch) (*PreparedBatch, error) {
	if !b.sharesSequence(next) {
		return nil, fmt.Errorf("%w: batches must use the same sequence handle", faults.ErrInvalidConfiguration)
	}
	first, second := b.batch.request, next.batch.request
	if !proto.Equal(first.CorrelationId, second.CorrelationId) || !proto.Equal(first.CausedBy, second.CausedBy) {
		return nil, fmt.Errorf("%w: batch correlation and actor must remain fixed", faults.ErrInvalidConfiguration)
	}
	explicit, err := mergeExplicitScopes(b.explicit, next.explicit)
	if err != nil {
		return nil, err
	}
	guards := slices.Concat(b.guards, next.guards)
	if err := validateDecisionScopes(explicit, guards); err != nil {
		return nil, err
	}
	// Prepared requests are never mutated (dispatch deep-clones), so the merged
	// header shares nested messages instead of deep-copying every staged event.
	request := &sequences.AppendManyForEventSourcesRequest{
		EventStore: first.EventStore, Namespace: first.Namespace, EventSequenceId: first.EventSequenceId,
		Events: slices.Concat(first.Events, second.Events), CorrelationId: first.CorrelationId,
		Tags: slices.Clone(first.Tags), Causation: slices.Clone(first.Causation), CausedBy: first.CausedBy,
		ConcurrencyScopes: slices.Clone(first.ConcurrencyScopes),
	}
	return &PreparedBatch{sequence: b.sequence, entries: slices.Concat(b.entries, next.entries), explicit: explicit, guards: guards,
		batch: preparedBatch{request: request, refs: slices.Concat(b.batch.refs, next.batch.refs), named: slices.Concat(b.batch.named, next.batch.named), audit: b.batch.audit}}, nil
}

func (b *PreparedBatch) sharesSequence(next *PreparedBatch) bool {
	return b != nil && next != nil && b.sequence != nil && b.sequence == next.sequence
}

func mergeExplicitScopes(first, second []LabeledScope) ([]LabeledScope, error) {
	explicit := slices.Clone(first)
	for _, incoming := range second {
		index := slices.IndexFunc(explicit, func(existing LabeledScope) bool { return existing.Label == incoming.Label })
		if index < 0 {
			explicit = append(explicit, incoming)
			continue
		}
		if !equalScopes(explicit[index].Scope, incoming.Scope) {
			return nil, fmt.Errorf("%w: conflicting concurrency scopes for label %q", faults.ErrInvalidConfiguration, incoming.Label)
		}
	}
	return explicit, nil
}

func equalScopes(first, second Scope) bool {
	return first.Expectation == second.Expectation && equalScopeFilterBounds(first.Filter, second.Filter) &&
		maps.Equal(scopeEventTypes(first.Filter.EventTypes), scopeEventTypes(second.Filter.EventTypes))
}

func equalScopeFilterBounds(first, second ScopeFilter) bool {
	return equalPointer(first.SourceID, second.SourceID) && equalPointer(first.SourceType, second.SourceType) &&
		equalPointer(first.StreamType, second.StreamType) && equalPointer(first.StreamID, second.StreamID)
}

func scopeEventTypes(refs []events.TypeRef) map[events.TypeRef]bool {
	result := make(map[events.TypeRef]bool)
	for _, ref := range refs {
		result[ref] = true
	}
	return result
}

func equalPointer[T comparable](first, second *T) bool {
	return (first == nil && second == nil) || (first != nil && second != nil && *first == *second)
}

// GetEvents returns owned copies of the serialized events in enrollment order.
// These are JSON snapshots, not the original Go values or persisted events.
func (b *PreparedBatch) GetEvents() []json.RawMessage {
	if b == nil || b.batch.request == nil {
		return nil
	}
	result := make([]json.RawMessage, len(b.batch.request.Events))
	for i, event := range b.batch.request.Events {
		result[i] = json.RawMessage(event.Content)
	}
	return result
}

// AppendPreparedBatch resolves remaining optimistic scopes and dispatches one
// atomic heterogeneous batch. No event is serialized again. The snapshot must
// originate from s; commit metadata comes from the snapshot, not ctx. This low-
// level method does not prevent repeated calls; transaction owners do.
// Local notification origin is resolved from this append ctx, not the snapshot.
// PrepareBatch never invokes the resolver; SDK-owned unit commits bypass it.
func (s *Sequence) AppendPreparedBatch(ctx context.Context, snapshot *PreparedBatch) (BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	if !s.ownsPreparedBatch(snapshot) {
		return BatchResult{}, faults.ErrInvalidConfiguration
	}
	origin, err := s.preparedBatchOrigin(ctx, snapshot)
	if err != nil {
		return BatchResult{Disposition: Rejected}, err
	}
	ctx = snapshot.batch.audit.Context(ctx)
	if len(snapshot.guards) > 0 {
		return s.appendDecisionBatch(ctx, snapshot, origin)
	}
	scopes, err := s.automaticBatchScopes(ctx, snapshot.entries, snapshot.explicit)
	if err != nil {
		return BatchResult{}, err
	}
	batch := snapshot.batch
	batch.request = proto.CloneOf(batch.request)
	ctx = metadata.WithCorrelation(ctx, wire.Correlation(batch.request.CorrelationId))
	batch, err = s.resolveBatch(ctx, batch, scopes)
	if err != nil {
		return BatchResult{}, err
	}
	return s.dispatchBatch(ctx, batch, origin)
}

func (s *Sequence) ownsPreparedBatch(snapshot *PreparedBatch) bool {
	return snapshot != nil && snapshot.sequence == s && s != nil
}

func (s *Sequence) preparedBatchOrigin(ctx context.Context, snapshot *PreparedBatch) (Origin, error) {
	if origin, owned := appendorigin.Unit[Origin](ctx, snapshot); owned {
		return origin, nil
	}
	return s.resolveAppendOrigin(ctx)
}
