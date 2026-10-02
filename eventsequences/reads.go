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
	"github.com/cratis/chronicle.go/internal/wire"
)

// ReadSource returns complete matching source history in ascending sequence order.
// Empty filters do not narrow; returned data is owned by the caller. A malformed
// response or failed envelope returns an error, never a partially trusted history.
func (s *Sequence) ReadSource(ctx context.Context, source events.SourceID, filter SourceFilter) ([]events.Appended, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	normalized, err := sourceReadFilter(source, filter)
	if err != nil {
		return nil, err
	}
	return s.readSource(ctx, normalized)
}

func (s *Sequence) readSource(ctx context.Context, filter ScopeFilter) ([]events.Appended, error) {
	response, err := s.service.ForEventSourceIdAndEventTypes(ctx, &sequences.ForEventSourceIdAndEventTypesRequest{EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id), EventSourceId: stringValue(filter.SourceID), EventSourceType: stringValue(filter.SourceType), EventStreamType: stringValue(filter.StreamType), EventStreamId: stringValue(filter.StreamID), EventTypeIds: typeIDs(filter.EventTypes)})
	if err != nil {
		return nil, err
	}
	return s.readResult(response, 0, filter)
}

// ReadHistory returns ordered events and a normalized filter/expectation suitable
// for saving work based on precisely that history. Empty history protects absence;
// nonempty history uses the greatest loaded position. No tail RPC is performed.
func (s *Sequence) ReadHistory(ctx context.Context, source events.SourceID, filter SourceFilter) (History, error) {
	if err := ctx.Err(); err != nil {
		return History{}, err
	}
	normalized, err := sourceReadFilter(source, filter)
	if err != nil {
		return History{}, err
	}
	loaded, err := s.readSource(ctx, normalized)
	if err != nil {
		return History{}, err
	}
	expectation := NoMatchingEvent()
	if len(loaded) > 0 {
		expectation = Exact(loaded[len(loaded)-1].Context.SequenceNumber)
	}
	return History{Events: loaded, Filter: normalized, Expectation: expectation}, nil
}

// ReadFrom returns matching events at or after from (inclusive), in ascending
// sequence order. The RPC is a finite unpaged read, not a live subscription.
func (s *Sequence) ReadFrom(ctx context.Context, from events.SequenceNumber, filter FromFilter) ([]events.Appended, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if from >= events.Unavailable-2 {
		return nil, fmt.Errorf("%w: reserved read position", faults.ErrInvalidConfiguration)
	}
	normalized, err := normalizeReadFilter(ScopeFilter{SourceID: filter.SourceID, EventTypes: filter.EventTypes})
	if err != nil {
		return nil, err
	}
	response, err := s.service.FromSequenceNumber(ctx, &sequences.FromSequenceNumberRequest{EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id), FromEventSequenceNumber: uint64(from), EventSourceId: stringValue(normalized.SourceID), EventTypeIds: typeIDs(normalized.EventTypes)})
	if err != nil {
		return nil, err
	}
	return s.readResult(response, from, normalized)
}

// Tail returns the greatest matching position and whether one exists. An empty
// history returns (0, false, nil), distinct from a first event at position zero.
// The returned position is not reserved and cannot protect an earlier read.
func (s *Sequence) Tail(ctx context.Context, filter TailFilter) (events.SequenceNumber, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	normalized, err := normalizeReadFilter(ScopeFilter(filter))
	if err != nil {
		return 0, false, err
	}
	return s.tail(ctx, normalized)
}

func (s *Sequence) tail(ctx context.Context, filter ScopeFilter) (events.SequenceNumber, bool, error) {
	response, err := s.service.TailSequenceNumber(ctx, &sequences.TailSequenceNumberRequest{EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id), EventSourceId: stringValue(filter.SourceID), EventSourceType: stringValue(filter.SourceType), EventStreamType: stringValue(filter.StreamType), EventStreamId: stringValue(filter.StreamID), EventTypeIds: typeIDs(filter.EventTypes)})
	if err != nil {
		return 0, false, err
	}
	if err = wire.CheckEnvelope(response); err != nil {
		return 0, false, err
	}
	if err = wire.RequireMessage(response, "Data"); err != nil {
		return 0, false, err
	}
	position := events.SequenceNumber(response.Data.SequenceNumber)
	if position == events.Unavailable {
		return 0, false, nil
	}
	if position >= events.Unavailable-2 {
		return 0, false, faults.ErrProtocol
	}
	return position, true, nil
}

// Next returns a non-reserved candidate position: zero for an empty sequence,
// otherwise tail plus one. A concurrent writer may consume it immediately.
func (s *Sequence) Next(ctx context.Context) (events.SequenceNumber, error) {
	tail, exists, err := s.Tail(ctx, TailFilter{})
	if err != nil || !exists {
		return 0, err
	}
	if tail+1 >= events.Unavailable-2 {
		return 0, fmt.Errorf("%w: sequence position exhausted", faults.ErrProtocol)
	}
	return tail + 1, nil
}

// HasEvents reports whether the source has any events in this sequence/namespace.
func (s *Sequence) HasEvents(ctx context.Context, source events.SourceID) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if strings.TrimSpace(string(source)) == "" {
		return false, fmt.Errorf("%w: source is required", faults.ErrInvalidConfiguration)
	}
	response, err := s.service.HasEventsForEventSourceId(ctx, &sequences.HasEventsForEventSourceIdRequest{EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id), EventSourceId: string(source)})
	if err != nil {
		return false, err
	}
	if err = wire.CheckEnvelope(response); err != nil {
		return false, err
	}
	if err = wire.RequireMessage(response, "Data"); err != nil {
		return false, err
	}
	return response.Data.HasEvents, nil
}
