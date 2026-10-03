// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/appendorigin"
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
}

// PrepareBatch snapshots entries, context metadata and scopes without I/O.
// Serialization and all local validation happen now; Resolve tails are read only
// by AppendPreparedBatch. Empty snapshots are allowed for incremental enrollment.
// Inputs are borrowed until return and must not be mutated concurrently.
func (s *Sequence) PrepareBatch(ctx context.Context, entries []Entry, options ...BatchOption) (*PreparedBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || s.catalog == nil || s.service == nil {
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
	// Every stage has its own audit chain. Do not let an empty per-entry chain
	// inherit a different stage's common chain when snapshots are joined.
	for _, event := range batch.request.Events {
		if len(event.Causation) == 0 {
			event.Causation = batch.request.Causation
		}
	}
	batch.request.Causation = nil
	result := &PreparedBatch{sequence: s, batch: batch}
	for _, entry := range entries {
		result.entries = append(result.entries, Entry{Source: entry.Source, Route: entry.Route})
	}
	for _, labeled := range config.scopes {
		result.explicit = append(result.explicit, LabeledScope{Label: labeled.Label, Scope: cloneScope(labeled.Scope)})
	}
	return result, nil
}

// Merge returns an ordered snapshot of b followed by next, without changing
// either input. Both must use the same Sequence handle, correlation and actor.
// Explicit scopes replace implicit defaults. Repeated identical scopes join;
// conflicting scopes fail atomically. Event-type filters compare as sets.
func (b *PreparedBatch) Merge(next *PreparedBatch) (*PreparedBatch, error) {
	if b == nil || next == nil || b.sequence == nil || b.sequence != next.sequence {
		return nil, fmt.Errorf("%w: batches must use the same sequence handle", faults.ErrInvalidConfiguration)
	}
	first, second := b.batch.request, next.batch.request
	if !proto.Equal(first.CorrelationId, second.CorrelationId) || !proto.Equal(first.CausedBy, second.CausedBy) {
		return nil, fmt.Errorf("%w: batch correlation and actor must remain fixed", faults.ErrInvalidConfiguration)
	}
	explicit := slices.Clone(b.explicit)
	for _, incoming := range next.explicit {
		index := slices.IndexFunc(explicit, func(existing LabeledScope) bool { return existing.Label == incoming.Label })
		if index < 0 {
			explicit = append(explicit, incoming)
			continue
		}
		if !equalScopes(explicit[index].Scope, incoming.Scope) {
			return nil, fmt.Errorf("%w: conflicting concurrency scopes for label %q", faults.ErrInvalidConfiguration, incoming.Label)
		}
	}
	// Prepared requests are never mutated (dispatch deep-clones), so the merged
	// header shares nested messages instead of deep-copying every staged event.
	request := &sequences.AppendManyForEventSourcesRequest{
		EventStore: first.EventStore, Namespace: first.Namespace, EventSequenceId: first.EventSequenceId,
		Events: slices.Concat(first.Events, second.Events), CorrelationId: first.CorrelationId,
		Tags: slices.Clone(first.Tags), Causation: slices.Clone(first.Causation), CausedBy: first.CausedBy,
		ConcurrencyScopes: slices.Clone(first.ConcurrencyScopes),
	}
	return &PreparedBatch{sequence: b.sequence, entries: slices.Concat(b.entries, next.entries), explicit: explicit,
		batch: preparedBatch{request: request, refs: slices.Concat(b.batch.refs, next.batch.refs), named: slices.Concat(b.batch.named, next.batch.named)}}, nil
}

func equalScopes(first, second Scope) bool {
	if first.Expectation != second.Expectation || !equalPointer(first.Filter.SourceID, second.Filter.SourceID) ||
		!equalPointer(first.Filter.SourceType, second.Filter.SourceType) || !equalPointer(first.Filter.StreamType, second.Filter.StreamType) ||
		!equalPointer(first.Filter.StreamID, second.Filter.StreamID) {
		return false
	}
	left, right := make(map[events.TypeRef]bool), make(map[events.TypeRef]bool)
	for _, ref := range first.Filter.EventTypes {
		left[ref] = true
	}
	for _, ref := range second.Filter.EventTypes {
		right[ref] = true
	}
	if len(left) != len(right) {
		return false
	}
	for ref := range left {
		if !right[ref] {
			return false
		}
	}
	return true
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
	if snapshot == nil || snapshot.sequence != s || s == nil {
		return BatchResult{}, faults.ErrInvalidConfiguration
	}
	origin, owned := appendorigin.Unit[Origin](ctx, snapshot)
	if !owned {
		var err error
		origin, err = s.resolveAppendOrigin(ctx)
		if err != nil {
			return BatchResult{Disposition: Rejected}, err
		}
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
