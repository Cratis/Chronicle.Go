// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package constraints describes kernel append-time constraint violations.
// Declaration authoring is not yet implemented; no client-side pre-check substitutes for enforcement.
package constraints

import "github.com/cratis/chronicle.go/events"

// Type preserves the kernel constraint kind, including unknown future values.
type Type int32

const (
	// Unknown is an unclassified violation.
	Unknown Type = iota
	// Unique is cross-source value uniqueness.
	Unique
	// UniqueEventType is event-type uniqueness within one source lifecycle.
	UniqueEventType
	// Schema is an event schema violation.
	Schema
	// StreamClosed rejects an append into a completed stream.
	StreamClosed
)

// Violation contains the complete constraint rejection diagnostic.
type Violation struct {
	// EventTypeID identifies the rejected event.
	EventTypeID events.TypeID
	// SequenceNumber is the kernel-supplied position or sentinel.
	SequenceNumber events.SequenceNumber
	// Type identifies the constraint kind.
	Type Type
	// ConstraintName is the stable constraint identity, not its display message.
	ConstraintName string
	// Message is the kernel's diagnostic text.
	Message string
	// Details contains exact detail keys/values, owned by the result.
	Details map[string]string
}
