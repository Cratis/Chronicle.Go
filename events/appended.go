// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"encoding/json"
	"time"

	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
)

// Appended owns raw JSON and metadata returned by an event read. Reads do not
// guess a Go type or generation. Use json.Unmarshal with the corresponding event
// shape; original and generational content are preserved for explicit decoding.
type Appended struct {
	// ID is the kernel's event identifier.
	ID string
	// Context describes the persisted event and selected store coordinates.
	Context Context
	// Content is the current JSON representation, independent of catalog registration.
	Content json.RawMessage
	// OriginalContent is the original JSON, or nil when absent on the wire.
	OriginalContent json.RawMessage
	// Revisions is the ordered revision history returned by the kernel.
	Revisions []Revision
	// GenerationalContent holds the available representations keyed by generation.
	GenerationalContent map[Generation]json.RawMessage
}

// Context contains persisted event metadata. Its slices and maps belong to the
// read result, not shared client state; callers may modify their own result.
type Context struct {
	// Store identifies the logical event store.
	Store metadata.StoreName
	// Namespace identifies the isolated namespace.
	Namespace metadata.Namespace
	// Sequence identifies the event sequence.
	Sequence SequenceID
	// EventType identifies the persisted generation.
	EventType TypeRef
	// Tombstone preserves the kernel's tombstone type flag.
	Tombstone bool
	// SourceType classifies the event source.
	SourceType SourceType
	// SourceID identifies the event source.
	SourceID SourceID
	// StreamType classifies the stream.
	StreamType StreamType
	// StreamID identifies the stream independently of the source.
	StreamID StreamID
	// SequenceNumber is the sequence-wide position; zero is a valid first event.
	SequenceNumber SequenceNumber
	// Occurred is the persisted occurrence timestamp with its numeric offset.
	Occurred time.Time
	// CorrelationID identifies the operation that appended this event.
	CorrelationID metadata.CorrelationID
	// Causation is the ordered audit chain, including per-entry additions.
	Causation []metadata.Causation
	// CausedBy is the actor and its ordered on-behalf-of chain.
	CausedBy identities.Identity
	// Tags contains ordinary event tags.
	Tags []Tag
	// NamedTags preserves structured exact name/value pairs.
	NamedTags []NamedTag
	// Subject is the compliance subject, defaulting to SourceID if absent.
	Subject Subject
	// Hash is the kernel's persisted content hash.
	Hash string
	// ObservationState preserves the wire state, including future values.
	ObservationState ObservationState
}

// ObservationState is the kernel's observation classification.
type ObservationState int32

const (
	// ObservationNone represents no observation classification.
	ObservationNone ObservationState = iota
	// ObservationInitial represents ordinary observation.
	ObservationInitial
	// ObservationReplay represents replay observation.
	ObservationReplay
)

// Revision preserves one historical revision without discarding its audit metadata.
type Revision struct {
	// Generation identifies the revision's schema generation.
	Generation Generation
	// CorrelationID identifies the revision operation.
	CorrelationID metadata.CorrelationID
	// CausedBy identifies the actor responsible for the revision.
	CausedBy identities.Identity
	// Occurred is the revision timestamp.
	Occurred time.Time
	// Content is the revision's JSON representation.
	Content json.RawMessage
}
