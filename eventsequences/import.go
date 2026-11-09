// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"errors"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/events"
	"github.com/google/uuid"
)

// ImportID identifies an import session. Its zero value means not set. Text and
// JSON use the canonical UUID form; the wire uses the .NET Guid layout.
type ImportID uuid.UUID

// NewImportID creates a random ImportID using cryptographic randomness.
func NewImportID() (ImportID, error) {
	id, err := uuid.NewRandom()
	return ImportID(id), err
}

// ParseImportID parses a UUID in one of the forms accepted by google/uuid.
func ParseImportID(value string) (ImportID, error) {
	id, err := uuid.Parse(value)
	return ImportID(id), err
}

// String returns the canonical UUID representation.
func (id ImportID) String() string { return uuid.UUID(id).String() }

// IsZero reports whether id is the unset value.
func (id ImportID) IsZero() bool { return id == ImportID{} }

// MarshalText implements encoding.TextMarshaler.
func (id ImportID) MarshalText() ([]byte, error) { return uuid.UUID(id).MarshalText() }

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *ImportID) UnmarshalText(data []byte) error {
	var value uuid.UUID
	if err := value.UnmarshalText(data); err != nil {
		return err
	}
	*id = ImportID(value)
	return nil
}

// IdempotencyKey identifies an imported event in its source system, typically
// the source's event ID. Re-importing an event with the same key into the same
// sequence reports it as already appended instead of appending it again. The
// empty key means the event is not deduplicated.
type IdempotencyKey string

// ImportState is the lifecycle state of an import session. Values match the
// wire contract; unknown future values are preserved.
type ImportState int32

const (
	// ImportOpen means the import is open and has received no chunk yet.
	ImportOpen ImportState = iota
	// ImportStaging means the import has received chunks and accepts more.
	ImportStaging
	// ImportValidating means staging is closed and the whole import is being validated.
	ImportValidating
	// ImportCommitting means the staged events are being committed.
	ImportCommitting
	// ImportCompleted means every staged event was committed or reported.
	ImportCompleted
	// ImportFailed means the import failed; when it failed before committing, nothing was committed.
	ImportFailed
	// ImportAbandoned means the import was abandoned before its commit started.
	ImportAbandoned
)

// String returns the state's name as the kernel spells it.
func (s ImportState) String() string {
	names := [...]string{"Open", "Staging", "Validating", "Committing", "Completed", "Failed", "Abandoned"}
	if s >= 0 && int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("ImportState(%d)", int32(s))
}

// IsTerminal reports whether the import can make no further progress.
func (s ImportState) IsTerminal() bool {
	return s == ImportCompleted || s == ImportFailed || s == ImportAbandoned
}

// OnInvalidEvent selects what an import does when one of its events is invalid.
type OnInvalidEvent int32

const (
	// RejectInvalidEvents fails the whole import and commits nothing. The default.
	RejectInvalidEvents OnInvalidEvent = 0
	// SkipInvalidEvents leaves invalid events out, reports them and commits the rest.
	SkipInvalidEvents OnInvalidEvent = 1
)

// ImportRejection reports an event of an import that was not staged or not committed.
type ImportRejection struct {
	// Index is the event's position in the import, counting from zero in the order it was added.
	Index int64
	// EventType is the event type, as its identifier and generation.
	EventType string
	// Reasons explains why the event was rejected.
	Reasons []string
}

// ImportProgress reports how far an import has come, as the kernel counts it.
type ImportProgress struct {
	// ImportID identifies the import.
	ImportID ImportID
	// State is the import's lifecycle state.
	State ImportState
	// EventsReceived counts the events the kernel has received.
	EventsReceived int64
	// EventsStaged counts the staged events.
	EventsStaged int64
	// EventsCommitted counts the committed events.
	EventsCommitted int64
	// AlreadyAppended counts events already appended under their idempotency key.
	AlreadyAppended int64
	// EventsRejected counts the rejected events.
	EventsRejected int64
}

// ImportResult is the outcome of an import. A failed or abandoned import is a
// result, not an operation error: inspect State, Rejections and FailureReason.
// The result owns its Rejections slice.
type ImportResult struct {
	// ImportID identifies the import.
	ImportID ImportID
	// State is the state the import reached.
	State ImportState
	// EventsStaged counts the staged events.
	EventsStaged int64
	// EventsCommitted counts the committed events.
	EventsCommitted int64
	// AlreadyAppended counts events already appended under their idempotency
	// key, and so not appended again.
	AlreadyAppended int64
	// FirstSequenceNumber is the first committed position, or events.Unavailable
	// when the import committed nothing.
	FirstSequenceNumber events.SequenceNumber
	// LastSequenceNumber is the last committed position, or events.Unavailable
	// when the import committed nothing.
	LastSequenceNumber events.SequenceNumber
	// RejectedCount counts every rejected event, including any beyond Rejections.
	RejectedCount int64
	// Rejections lists the rejected events the kernel retained.
	Rejections []ImportRejection
	// FailureReason explains why the import failed or was abandoned, if it did.
	FailureReason string
}

// IsSuccess reports whether the import completed.
func (r ImportResult) IsSuccess() bool { return r.State == ImportCompleted }

func (r ImportResult) clone() ImportResult {
	r.Rejections = slices.Clone(r.Rejections)
	for i := range r.Rejections {
		r.Rejections[i].Reasons = slices.Clone(r.Rejections[i].Reasons)
	}
	return r
}

// ErrImportAbandoned reports adding to or completing an import after Abandon.
// Inspect it with errors.Is.
var ErrImportAbandoned = errors.New("chronicle: import was abandoned")

// ImportChunkRefusedError reports that the kernel refused a chunk as a whole.
// The session sends nothing more; reattach to continue from the last
// acknowledged chunk once the cause is fixed.
type ImportChunkRefusedError struct {
	// ChunkIndex is the refused chunk.
	ChunkIndex int32
	// Reason is the kernel's explanation.
	Reason string
}

func (e *ImportChunkRefusedError) Error() string {
	return fmt.Sprintf("chronicle: import chunk %d was refused: %s", e.ChunkIndex, e.Reason)
}

// ErrImportNotFound reports an import ID the kernel does not know. Inspect it
// with errors.Is.
var ErrImportNotFound = errors.New("chronicle: import not found")

// ImportCouldNotBeginError reports that the kernel refused to begin or reattach an import.
type ImportCouldNotBeginError struct {
	// Reason is the kernel's or client's explanation.
	Reason string
	// Err is the stable category, such as ErrImportNotFound, or nil.
	Err error
}

func (e *ImportCouldNotBeginError) Error() string {
	return "chronicle: import could not begin: " + e.Reason
}

// Unwrap returns the stable category, if any.
func (e *ImportCouldNotBeginError) Unwrap() error { return e.Err }
