// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package observation defines observer completion coordinates. Waiting and observer administration are not yet implemented.
package observation

import (
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
)

// CompletionTarget identifies committed positions without retaining a client.
// Zero-valued targets do not represent committed work.
type CompletionTarget struct {
	// Store identifies the event store.
	Store metadata.StoreName
	// Namespace identifies the isolated namespace.
	Namespace metadata.Namespace
	// Sequence identifies the event sequence.
	Sequence events.SequenceID
	// First is the first committed position; nil means no committed work.
	First *events.SequenceNumber
	// EventTypeTails maps each appended event type to its last committed position.
	EventTypeTails map[events.TypeID]events.SequenceNumber
}
