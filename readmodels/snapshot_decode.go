// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"encoding/json"
	"maps"
	"strings"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

func (s *Service) snapshotEvent(model Descriptor, event *contracts.Event) (events.Appended, error) {
	if event == nil || event.Context == nil || event.Context.EventType == nil || !validDocument([]byte(event.Content)) {
		return events.Appended{}, faults.ErrProtocol
	}
	c := event.Context
	if c.EventType.Id == "" || c.EventType.Generation == 0 || c.SequenceNumber >= uint64(events.Unavailable-2) {
		return events.Appended{}, faults.ErrProtocol
	}
	occurred, err := snapshotTime(c.Occurred)
	if err != nil {
		return events.Appended{}, err
	}
	result := events.Appended{Content: json.RawMessage(event.Content), Context: events.Context{
		Store: s.store, Namespace: s.namespace, Sequence: model.EventSequence(),
		EventType: events.TypeRef{ID: events.TypeID(c.EventType.Id), Generation: events.Generation(c.EventType.Generation)},
		Tombstone: c.EventType.Tombstone, SourceType: events.SourceType(c.EventSourceType), SourceID: events.SourceID(c.EventSourceId),
		SequenceNumber: events.SequenceNumber(c.SequenceNumber), StreamType: events.StreamType(c.EventStreamType), StreamID: events.StreamID(c.EventStreamId),
		Occurred: occurred, CorrelationID: wire.Correlation(c.CorrelationId), CausedBy: snapshotIdentity(c.CausedBy),
		Hash: c.Hash, ObservationState: events.ObservationState(c.ObservationState), Subject: events.Subject(c.Subject),
	}}
	if result.Context.Subject == "" {
		result.Context.Subject = events.Subject(c.EventSourceId)
	}
	for _, cause := range c.Causation {
		if cause == nil {
			return events.Appended{}, faults.ErrProtocol
		}
		occurred, err := snapshotTime(cause.Occurred)
		if err != nil {
			return events.Appended{}, err
		}
		result.Context.Causation = append(result.Context.Causation, metadata.Causation{Occurred: occurred, Type: cause.Type, Properties: maps.Clone(cause.Properties)})
	}
	for _, tag := range c.Tags {
		result.Context.Tags = append(result.Context.Tags, events.Tag(tag))
	}
	for _, tag := range c.NamedTags {
		if tag == nil || strings.TrimSpace(tag.Name) == "" {
			return events.Appended{}, faults.ErrProtocol
		}
		result.Context.NamedTags = append(result.Context.NamedTags, events.NamedTag{Name: tag.Name, Value: tag.Value})
	}
	return result, nil
}

func snapshotTime(value *contracts.SerializableDateTimeOffset) (time.Time, error) {
	if value == nil {
		return time.Time{}, faults.ErrProtocol
	}
	parsed, err := time.Parse(time.RFC3339Nano, value.Value)
	if err != nil {
		return time.Time{}, faults.ErrProtocol
	}
	return parsed, nil
}

func snapshotIdentity(value *contracts.Identity) identities.Identity {
	if value == nil {
		return identities.Identity{}
	}
	result := identities.Identity{Subject: value.Subject, Name: value.Name, UserName: value.UserName}
	if value.OnBehalfOf != nil {
		nested := snapshotIdentity(value.OnBehalfOf)
		result.OnBehalfOf = &nested
	}
	return result
}
