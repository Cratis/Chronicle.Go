// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/metadata"
)

// importEvent is one event in its wire form, independent of the generated
// import contracts. Repeated and message values reuse the append contract's
// shapes, which the import contract mirrors field for field.
type importEvent struct {
	sourceID, sourceType, streamType, streamID string
	eventType                                  *sequences.EventType
	content                                    string
	tags                                       []string
	namedTags                                  []*sequences.NamedTag
	occurred                                   string
	subject                                    string
	correlation                                metadata.CorrelationID
	causation                                  []*sequences.Causation
	causedBy                                   *sequences.Identity
	idempotencyKey                             string
	sourceSequenceNumber                       *uint64
}

// size measures an event as the kernel counts it against its chunk byte limit:
// content, idempotency key, tags, named tags and causation, as UTF-8 bytes.
func (e *importEvent) size() int {
	bytes := len(e.content) + len(e.idempotencyKey)
	for _, tag := range e.tags {
		bytes += len(tag)
	}
	for _, tag := range e.namedTags {
		bytes += len(tag.GetName()) + len(tag.GetValue())
	}
	for _, cause := range e.causation {
		bytes += len(cause.GetType())
		for name, value := range cause.GetProperties() {
			bytes += len(name) + len(value)
		}
	}
	return bytes
}

type importOpened struct {
	id             ImportID
	maxChunkEvents int32
	maxChunkBytes  int32
	err            string
}

type importChunk struct {
	id              ImportID
	index           int32
	firstEventIndex int64
	events          []importEvent
}

type importChunkAck struct {
	index int32
	err   string
}

type importStatus struct {
	state ImportState
	err   string
}

// importSnapshot is the kernel's view of an import, as GetImport and
// ObserveImport report it.
type importSnapshot struct {
	result             ImportResult
	sequence           string
	onInvalid          OnInvalidEvent
	eventsReceived     int64
	maxChunkEvents     int32
	maxChunkBytes      int32
	acknowledgedChunks int32
	acknowledgedEvents int64
}

func (s importSnapshot) progress() ImportProgress {
	return ImportProgress{ImportID: s.result.ImportID, State: s.result.State, EventsReceived: s.eventsReceived, EventsStaged: s.result.EventsStaged,
		EventsCommitted: s.result.EventsCommitted, AlreadyAppended: s.result.AlreadyAppended, EventsRejected: s.result.RejectedCount}
}

// importTransport is the Imports service of one sequence. Errors are gRPC status
// errors, so the session can tell a transient failure from a refusal, or an
// envelope or protocol error that must not be retried.
type importTransport interface {
	begin(ctx context.Context, id ImportID, onInvalid OnInvalidEvent) (importOpened, error)
	addChunk(ctx context.Context, chunk importChunk) (importChunkAck, error)
	complete(ctx context.Context, id ImportID, expectedChunks int32, expectedEvents int64) (importStatus, error)
	abandon(ctx context.Context, id ImportID) (importStatus, error)
	// get reports false when the kernel knows no such import.
	get(ctx context.Context, id ImportID) (importSnapshot, bool, error)
	// observe calls yield for each update until yield returns false, the stream
	// ends or ctx is done.
	observe(ctx context.Context, id ImportID, yield func(importSnapshot) bool) error
}

func (s *Sequence) importService() importTransport {
	if s.imports != nil {
		return s.imports
	}
	return newImportTransport(s)
}
