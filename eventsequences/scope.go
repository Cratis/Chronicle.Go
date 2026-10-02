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

func (s *Sequence) resolveScope(ctx context.Context, source events.SourceID, config appendConfig) (*sequences.ConcurrencyScope, error) {
	scope := Scope{Filter: ScopeFilter{SourceID: &source, SourceType: &config.route.SourceType, StreamType: &config.route.StreamType, StreamID: &config.route.StreamID}}
	if config.scope != nil {
		scope = *config.scope
	}
	filter := scope.Filter
	if filter.SourceID != nil && (*filter.SourceID == "" || *filter.SourceID != source) {
		return nil, fmt.Errorf("%w: single append scope must name its target source", faults.ErrInvalidConfiguration)
	}
	if (filter.SourceType != nil && *filter.SourceType == "") || (filter.StreamType != nil && *filter.StreamType == "") || (filter.StreamID != nil && *filter.StreamID == "") {
		return nil, fmt.Errorf("%w: empty scope dimension", faults.ErrInvalidConfiguration)
	}
	if scope.Expectation.kind == 1 && scope.Expectation.position >= events.Unavailable-2 {
		return nil, fmt.Errorf("%w: reserved exact sequence number", faults.ErrInvalidConfiguration)
	}
	result := &sequences.ConcurrencyScope{SequenceNumber: uint64(events.Unavailable), EventSourceId: filter.SourceID != nil,
		EventSourceType: stringValue(filter.SourceType), EventStreamType: stringValue(filter.StreamType), EventStreamId: stringValue(filter.StreamID)}
	var ids []string
	for _, eventType := range filter.EventTypes {
		if eventType.ID == "" || strings.Contains(string(eventType.ID), ",") || eventType.Generation == 0 {
			return nil, fmt.Errorf("%w: invalid scope event type", faults.ErrInvalidConfiguration)
		}
		ids = append(ids, string(eventType.ID))
		result.EventTypes = append(result.EventTypes, &sequences.EventType{Id: string(eventType.ID), Generation: uint32(eventType.Generation)})
	}
	switch scope.Expectation.kind {
	case 0:
		envelope, err := s.service.TailSequenceNumber(ctx, &sequences.TailSequenceNumberRequest{
			EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id), EventSourceId: stringValue(filter.SourceID),
			EventSourceType: result.EventSourceType, EventStreamType: result.EventStreamType, EventStreamId: result.EventStreamId, EventTypeIds: strings.Join(ids, ","),
		})
		if err != nil {
			return nil, fmt.Errorf("chronicle: resolve concurrency tail: %w", err)
		}
		if err = wire.CheckEnvelope(envelope); err != nil {
			return nil, err
		}
		if err = wire.RequireMessage(envelope, "Data"); err != nil {
			return nil, err
		}
		result.SequenceNumber = envelope.Data.SequenceNumber
		if result.SequenceNumber != uint64(events.Unavailable) && result.SequenceNumber >= uint64(events.Unavailable-2) {
			return nil, faults.ErrProtocol
		}
	case 1:
		result.SequenceNumber = uint64(scope.Expectation.position)
	case 2:
		result.ExpectsNoMatchingEvent = true
	case 3:
		if filter.SourceID != nil || filter.SourceType != nil || filter.StreamType != nil || filter.StreamID != nil || len(filter.EventTypes) > 0 {
			return nil, fmt.Errorf("%w: NoCheck cannot carry narrowing", faults.ErrInvalidConfiguration)
		}
	default:
		return nil, faults.ErrInvalidConfiguration
	}
	return result, nil
}

func stringValue[T ~string](value *T) string {
	if value == nil {
		return ""
	}
	return string(*value)
}
