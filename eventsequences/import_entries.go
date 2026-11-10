// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/outgoing"
	"github.com/cratis/chronicle.go/metadata"
)

// importEncoder converts entries to their wire form with the metadata selected
// once for one Add call: identity, causation and a correlation shared by every
// entry that does not carry its own.
type importEncoder struct {
	sequence    *Sequence
	audit       outgoing.Audit
	correlation metadata.CorrelationID
}

func (s *Sequence) importEncoder(ctx context.Context) (importEncoder, error) {
	audit, err := s.outgoing.Resolve(ctx, metadata.Correlation(ctx), false, metadata.CorrelationID{})
	if err != nil {
		return importEncoder{}, err
	}
	correlation := audit.Correlation
	if correlation == (metadata.CorrelationID{}) {
		if correlation, err = metadata.NewCorrelationID(); err != nil {
			return importEncoder{}, err
		}
	}
	return importEncoder{sequence: s, audit: audit, correlation: correlation}, nil
}

// encode validates and serializes one entry. index is its position in the
// import, used in preparation errors. Inputs are borrowed for the call.
func (e importEncoder) encode(ctx context.Context, entry Entry, index int64) (importEvent, error) {
	s := e.sequence
	if strings.TrimSpace(string(entry.Source)) == "" {
		return importEvent{}, fmt.Errorf("%w: import event %d has no event source", faults.ErrInvalidConfiguration, index)
	}
	descriptor, found := s.catalog.Lookup(entry.Event)
	if !found {
		return importEvent{}, fmt.Errorf("%w: import event %d", faults.ErrNotRegistered, index)
	}
	if err := validateAppendMetadata(entry.Subject, entry.Occurred, entry.NamedTags); err != nil {
		return importEvent{}, err
	}
	if entry.IdempotencyKey != "" && strings.TrimSpace(string(entry.IdempotencyKey)) == "" {
		return importEvent{}, fmt.Errorf("%w: import event %d has a blank idempotency key", faults.ErrInvalidConfiguration, index)
	}
	if entry.SourceSequenceNumber != nil && !entry.SourceSequenceNumber.IsActualValue() {
		return importEvent{}, fmt.Errorf("%w: import event %d has a reserved source sequence number", faults.ErrInvalidConfiguration, index)
	}
	causes := append(slices.Clone(e.audit.Causes), metadata.CausationChain(metadata.WithCausationChain(context.Background(), entry.Causation))...)
	for _, cause := range causes {
		if cause.Occurred.Year() < 1 || cause.Occurred.Year() > 9999 {
			return importEvent{}, fmt.Errorf("%w: import event %d has a causation outside the .NET date range", faults.ErrInvalidConfiguration, index)
		}
	}
	position := int(min(index, int64(^uint(0)>>1)))
	subject, err := frozenSubject(ctx, descriptor, entry.Event, entry.Source, entry.Subject, position)
	if err != nil {
		return importEvent{}, err
	}
	entryCtx := metadata.WithCausationChain(e.audit.Context(ctx), causes)
	content, err := s.outgoing.Encode(entryCtx, descriptor, entry.Event, entry.Subject != nil, position)
	if err != nil {
		return importEvent{}, err
	}
	config := appendConfig{route: entry.Route, scope: &Scope{Expectation: NoCheck()}, correlation: e.correlation, subject: &subject, occurred: entry.Occurred, tags: entry.Tags}
	request, err := s.request(entryCtx, entry.Source, descriptor, string(content), config)
	if err != nil {
		return importEvent{}, err
	}
	event := importEvent{sourceID: request.EventSourceId, sourceType: request.EventSourceType, streamType: request.EventStreamType, streamID: request.EventStreamId,
		eventType: &sequences.EventType{Id: request.EventType.GetId(), Generation: request.EventType.GetGeneration()}, content: request.Content, tags: request.Tags,
		namedTags: namedRequest(request, entry.NamedTags).NamedTags, occurred: request.Occurred.GetValue(), subject: request.Subject, correlation: e.correlation,
		causation: request.Causation, causedBy: request.CausedBy, idempotencyKey: string(entry.IdempotencyKey)}
	if entry.CorrelationID != (metadata.CorrelationID{}) {
		event.correlation = entry.CorrelationID
	}
	if entry.CausedBy != nil {
		event.causedBy = identityContract(*entry.CausedBy)
	}
	if entry.Generation != 0 {
		event.eventType.Generation = uint32(entry.Generation)
	}
	if entry.SourceSequenceNumber != nil {
		number := uint64(*entry.SourceSequenceNumber)
		event.sourceSequenceNumber = &number
	}
	return event, nil
}
