// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/outgoing"
)

func singleRequest(batch *sequences.AppendManyForEventSourcesRequest, event *sequences.EventForEventSourceId) *sequences.AppendRequest {
	causes := event.Causation
	if len(causes) == 0 {
		causes = batch.Causation
	}
	return &sequences.AppendRequest{EventStore: batch.EventStore, Namespace: batch.Namespace, EventSequenceId: batch.EventSequenceId,
		EventSourceId: event.EventSourceId, EventSourceType: event.EventSourceType, EventStreamType: event.EventStreamType, EventStreamId: event.EventStreamId,
		EventType: event.EventType, Content: event.Content, Tags: event.Tags, Subject: event.Subject, Occurred: event.Occurred,
		CorrelationId: batch.CorrelationId, CausedBy: batch.CausedBy, Causation: causes}
}

// BindAuditMetadata is the internal Begin bridge. Empty preparation resolves
// metadata without enriching any events; no caller context is retained.
func (s *Sequence) BindAuditMetadata(_ outgoing.Binding, ctx context.Context) (*PreparedBatch, outgoing.Audit, error) {
	pending, err := s.PrepareBatch(ctx, nil)
	if err != nil {
		return nil, outgoing.Audit{}, err
	}
	return pending, pending.batch.audit, nil
}

// PrepareBoundBatch resolves Stage's operational metadata outside unit locks,
// verifies its bound actor/correlation before encoding, and snapshots all entries.
func (s *Sequence) PrepareBoundBatch(_ outgoing.Binding, ctx context.Context, bound outgoing.Audit, entries []Entry, scopes []LabeledScope) (*PreparedBatch, error) {
	if s == nil {
		return nil, faults.ErrInvalidConfiguration
	}
	return s.PrepareBatch(ctx, entries, WithScopes(scopes...), func(c *batchConfig) {
		c.inheritedCorrelation, c.bound = bound.Correlation, &bound
	})
}
