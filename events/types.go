// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package events defines persisted event identities and immutable type catalogs.
package events

// TypeID is a stable persisted event identifier, not necessarily a UUID.
type TypeID string

// Generation identifies an event schema generation, beginning at one.
type Generation uint32

// SourceID identifies the entity or subject whose event is appended.
type SourceID string

// SourceType classifies an event source independently of its identity.
type SourceType string

// StreamType classifies a stream.
type StreamType string

// StreamID selects a stream independently of its source.
type StreamID string

// SequenceID identifies a sequence within a store namespace.
type SequenceID string

// SequenceNumber is a sequence-wide unsigned position, beginning at First (zero).
// Unavailable, Max and BeforeFirst are reserved, non-actual values. Use its checked
// methods rather than raw arithmetic to preserve sentinels and reject range errors.
type SequenceNumber uint64

// Subject is the compliance subject, not the causing actor.
type Subject string

// Tag is an opaque ordinary event tag.
type Tag string

const (
	// EventLog is the primary event sequence.
	EventLog SequenceID = "event-log"
	// DefaultSourceType is the default append source classification.
	DefaultSourceType SourceType = "Default"
	// AllStreamTypes is the default stream type and a non-narrowing read sentinel.
	AllStreamTypes StreamType = "All"
	// DefaultStreamID is the default stream; it is not the event source ID.
	DefaultStreamID StreamID = "Default"
	// First is the first actual sequence position and the zero value.
	First SequenceNumber = 0
	// Unavailable is the wire sentinel for an absent tail or unchecked expectation.
	Unavailable SequenceNumber = ^SequenceNumber(0)
	// Max is a reserved system value, not the largest actual position.
	Max SequenceNumber = Unavailable - 1
	// BeforeFirst represents a local expectation of no matching event. It is not
	// a wire position: use eventsequences.NoMatchingEvent for protected absence.
	BeforeFirst SequenceNumber = Unavailable - 2
)

// TypeRef identifies one exact persisted event generation.
type TypeRef struct {
	// ID is the stable event identifier.
	ID TypeID
	// Generation is the positive generation number.
	Generation Generation
}

// NamedTag preserves an exact name/value pair; an empty Value is valid.
type NamedTag struct {
	// Name must be nonblank.
	Name string
	// Value is opaque and may be empty.
	Value string
}
