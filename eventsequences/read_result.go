// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

func (s *Sequence) readResult(response *sequences.QueryResult_IEnumerable_AppendedEventResponse, from events.SequenceNumber, filter ScopeFilter) ([]events.Appended, error) {
	if err := wire.CheckEnvelope(response); err != nil {
		return nil, err
	}
	result := make([]events.Appended, len(response.Data))
	for i, event := range response.Data {
		converted, err := s.appended(event)
		if err != nil {
			return nil, fmt.Errorf("chronicle: read event %d: %w", i, err)
		}
		if converted.Context.SequenceNumber < from || !matchesRead(converted.Context, filter) {
			return nil, fmt.Errorf("%w: event outside requested history", faults.ErrProtocol)
		}
		result[i] = converted
	}
	slices.SortFunc(result, func(a, b events.Appended) int {
		if a.Context.SequenceNumber < b.Context.SequenceNumber {
			return -1
		}
		if a.Context.SequenceNumber > b.Context.SequenceNumber {
			return 1
		}
		return 0
	})
	for i := 1; i < len(result); i++ {
		if result[i-1].Context.SequenceNumber == result[i].Context.SequenceNumber {
			return nil, fmt.Errorf("%w: duplicate history position", faults.ErrProtocol)
		}
	}
	return result, nil
}

func matchesRead(ctx events.Context, filter ScopeFilter) bool {
	if filter.SourceID != nil && ctx.SourceID != *filter.SourceID {
		return false
	}
	if filter.SourceType != nil && ctx.SourceType != *filter.SourceType {
		return false
	}
	if filter.StreamType != nil && ctx.StreamType != *filter.StreamType {
		return false
	}
	if filter.StreamID != nil && ctx.StreamID != *filter.StreamID {
		return false
	}
	return len(filter.EventTypes) == 0 || slices.ContainsFunc(filter.EventTypes, func(ref events.TypeRef) bool { return ref.ID == ctx.EventType.ID })
}

func (s *Sequence) appended(event *sequences.AppendedEventResponse) (events.Appended, error) {
	if event == nil || event.Context == nil || event.Context.EventType == nil {
		return events.Appended{}, faults.ErrProtocol
	}
	ctx := event.Context
	if ctx.EventType.Id == "" || ctx.EventType.Generation == 0 || ctx.SequenceNumber >= uint64(events.Unavailable-2) {
		return events.Appended{}, faults.ErrProtocol
	}
	occurred, err := readTime(ctx.Occurred)
	if err != nil {
		return events.Appended{}, err
	}
	result := events.Appended{ID: event.Id, Context: events.Context{Store: s.store, Namespace: s.namespace, Sequence: s.id,
		EventType: events.TypeRef{ID: events.TypeID(ctx.EventType.Id), Generation: events.Generation(ctx.EventType.Generation)}, Tombstone: ctx.EventType.Tombstone,
		SourceID: events.SourceID(ctx.EventSourceId), SourceType: events.SourceType(ctx.EventSourceType), StreamType: events.StreamType(ctx.EventStreamType), StreamID: events.StreamID(ctx.EventStreamId), SequenceNumber: events.SequenceNumber(ctx.SequenceNumber), Occurred: occurred,
		CorrelationID: wire.Correlation(ctx.CorrelationId), CausedBy: readIdentity(ctx.CausedBy), Hash: ctx.Hash, ObservationState: events.ObservationState(ctx.ObservationState), Subject: events.Subject(ctx.Subject)}}
	if result.Context.Subject == "" {
		result.Context.Subject = events.Subject(ctx.EventSourceId)
	}
	if result.Content, err = readJSON(event.Content, false); err != nil {
		return events.Appended{}, err
	}
	if result.OriginalContent, err = readJSON(event.OriginalContent, true); err != nil {
		return events.Appended{}, err
	}
	for _, cause := range ctx.Causation {
		if cause == nil {
			return events.Appended{}, faults.ErrProtocol
		}
		occurred, err := readTime(cause.Occurred)
		if err != nil {
			return events.Appended{}, err
		}
		result.Context.Causation = append(result.Context.Causation, metadata.Causation{Occurred: occurred, Type: cause.Type, Properties: maps.Clone(cause.Properties)})
	}
	for _, tag := range ctx.Tags {
		result.Context.Tags = append(result.Context.Tags, events.Tag(tag))
	}
	for _, tag := range ctx.NamedTags {
		if tag == nil || strings.TrimSpace(tag.Name) == "" {
			return events.Appended{}, faults.ErrProtocol
		}
		result.Context.NamedTags = append(result.Context.NamedTags, events.NamedTag{Name: tag.Name, Value: tag.Value})
	}
	for _, revision := range event.Revisions {
		converted, err := readRevision(revision)
		if err != nil {
			return events.Appended{}, err
		}
		result.Revisions = append(result.Revisions, converted)
	}
	if len(event.GenerationalContent) > 0 {
		result.GenerationalContent = make(map[events.Generation]json.RawMessage)
	}
	for _, generation := range event.GenerationalContent {
		if generation == nil || generation.Key <= 0 {
			return events.Appended{}, faults.ErrProtocol
		}
		key := events.Generation(generation.Key)
		if _, duplicate := result.GenerationalContent[key]; duplicate {
			return events.Appended{}, faults.ErrProtocol
		}
		content, err := readJSON(generation.Value, false)
		if err != nil {
			return events.Appended{}, err
		}
		result.GenerationalContent[key] = content
	}
	return result, nil
}

func readRevision(revision *sequences.EventRevision) (events.Revision, error) {
	if revision == nil || revision.Generation == 0 {
		return events.Revision{}, faults.ErrProtocol
	}
	occurred, err := readTime(revision.Occurred)
	if err != nil {
		return events.Revision{}, err
	}
	id, err := metadata.ParseCorrelationID(revision.CorrelationId)
	if err != nil {
		return events.Revision{}, fmt.Errorf("%w: invalid revision correlation", faults.ErrProtocol)
	}
	content, err := readJSON(revision.Content, false)
	if err != nil {
		return events.Revision{}, err
	}
	return events.Revision{Generation: events.Generation(revision.Generation), CorrelationID: id, Occurred: occurred, CausedBy: readIdentity(revision.CausedBy), Content: content}, nil
}

func readTime(value *sequences.SerializableDateTimeOffset) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("%w: missing occurrence", faults.ErrProtocol)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value.Value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid occurrence", faults.ErrProtocol)
	}
	return parsed, nil
}

func readJSON(value string, optional bool) (json.RawMessage, error) {
	if value == "" && optional {
		return nil, nil
	}
	data := json.RawMessage(value)
	if !json.Valid(data) {
		return nil, fmt.Errorf("%w: invalid event JSON", faults.ErrProtocol)
	}
	return data, nil
}

func readIdentity(value *sequences.Identity) identities.Identity {
	if value == nil {
		return identities.Identity{}
	}
	result := identities.Identity{Subject: value.Subject, Name: value.Name, UserName: value.UserName}
	if value.OnBehalfOf != nil {
		nested := readIdentity(value.OnBehalfOf)
		result.OnBehalfOf = &nested
	}
	return result
}
