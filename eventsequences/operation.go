// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
)

// OperationMetadata identifies attempted work, not confirmed commitment. It is
// available even for rejected/unknown results and contains no completion target.
// Its zero value describes no operation. Event types retain their generations.
type OperationMetadata struct {
	store     metadata.StoreName
	namespace metadata.Namespace
	sequence  events.SequenceID
	refs      []events.TypeRef
	appended  []events.TypeRef // Exact input order, including repeated IDs/generations, for completion.
}

// Store returns the attempted logical store.
func (m OperationMetadata) Store() metadata.StoreName { return m.store }

// Namespace returns the attempted namespace.
func (m OperationMetadata) Namespace() metadata.Namespace { return m.namespace }

// Sequence returns the attempted event sequence.
func (m OperationMetadata) Sequence() events.SequenceID { return m.sequence }

// EventTypes returns a defensive copy of distinct exact types in input order.
func (m OperationMetadata) EventTypes() []events.TypeRef {
	return append([]events.TypeRef(nil), m.refs...)
}

// AppendOperationResult adds operation metadata without changing AppendResult's
// public struct layout. Result retains the existing disposition/diagnostics.
type AppendOperationResult struct {
	result    AppendResult
	operation OperationMetadata
}

// Result returns the append result. Diagnostics follow AppendResult's ownership contract.
func (r AppendOperationResult) Result() AppendResult { return r.result }

// Operation returns immutable metadata independently of the write outcome.
func (r AppendOperationResult) Operation() OperationMetadata { return r.operation }

// BatchOperationResult adds operation metadata without changing BatchResult's layout.
type BatchOperationResult struct {
	result    BatchResult
	operation OperationMetadata
}

// Result returns the existing batch result and diagnostics.
func (r BatchOperationResult) Result() BatchResult { return r.result }

// Operation returns immutable metadata independently of the write outcome.
func (r BatchOperationResult) Operation() OperationMetadata { return r.operation }

func (s *Sequence) operation(values []any) OperationMetadata {
	m := OperationMetadata{store: s.store, namespace: s.namespace, sequence: s.id}
	seen := make(map[events.TypeRef]bool)
	for _, value := range values {
		if descriptor, ok := s.catalog.Lookup(value); ok {
			m.appended = append(m.appended, descriptor.Ref())
			if !seen[descriptor.Ref()] {
				seen[descriptor.Ref()] = true
				m.refs = append(m.refs, descriptor.Ref())
			}
		}
	}
	return m
}

// AppendWithMetadata behaves like Append and also returns attempted coordinates
// and registered event types on every outcome, including operation errors.
// Unregistered inputs have no type reference; they still fail as in Append.
func (s *Sequence) AppendWithMetadata(ctx context.Context, source events.SourceID, event any, options ...AppendOption) (AppendOperationResult, error) {
	operation := s.operation([]any{event})
	result, err := s.Append(ctx, source, event, options...)
	return AppendOperationResult{result: result, operation: operation}, err
}

// AppendManyWithMetadata behaves like AppendMany with outcome-independent metadata.
func (s *Sequence) AppendManyWithMetadata(ctx context.Context, source events.SourceID, values []any, options ...AppendOption) (BatchOperationResult, error) {
	operation := s.operation(values)
	result, err := s.AppendMany(ctx, source, values, options...)
	return BatchOperationResult{result: result, operation: operation}, err
}

// AppendBatchWithMetadata behaves like AppendBatch with outcome-independent metadata.
func (s *Sequence) AppendBatchWithMetadata(ctx context.Context, entries []Entry, options ...BatchOption) (BatchOperationResult, error) {
	values := make([]any, len(entries))
	for i, entry := range entries {
		values[i] = entry.Event
	}
	operation := s.operation(values)
	result, err := s.AppendBatch(ctx, entries, options...)
	return BatchOperationResult{result: result, operation: operation}, err
}

// Operation returns attempted coordinates and exact types for an immutable
// prepared snapshot, without I/O. Nil/zero snapshots return zero metadata.
func (b *PreparedBatch) Operation() OperationMetadata {
	if b == nil || b.sequence == nil {
		return OperationMetadata{}
	}
	m := b.sequence.operation(nil)
	seen := make(map[events.TypeRef]bool)
	for _, ref := range b.batch.refs {
		m.appended = append(m.appended, ref)
		if !seen[ref] {
			seen[ref] = true
			m.refs = append(m.refs, ref)
		}
	}
	return m
}
