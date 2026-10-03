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

// CompleteStreamError is a known kernel refusal. Compare with errors.Is.
type CompleteStreamError int32

const (
	// StreamAlreadyCompleted means the stream stays closed; no new completion occurred.
	StreamAlreadyCompleted CompleteStreamError = 1
	// DefaultStreamCannotBeCompleted protects the All/Default stream.
	DefaultStreamCannotBeCompleted CompleteStreamError = 2
)

func (e CompleteStreamError) Error() string {
	switch e {
	case StreamAlreadyCompleted:
		return "chronicle: stream already completed"
	case DefaultStreamCannotBeCompleted:
		return "chronicle: default stream cannot be completed"
	default:
		return fmt.Sprintf("chronicle: stream completion error %d", e)
	}
}

// CompleteStream permanently closes a stream type/ID pair in this sequence,
// across ALL event sources. Both parameters must be explicit and nonblank.
// Success returns the sequence-wide tail at completion, not the stream's tail;
// Unavailable is valid for an empty sequence. Subsequent appends to the pair are
// rejected by the kernel with StreamClosed. Repeated completion returns
// StreamAlreadyCompleted. All/Default returns DefaultStreamCannotBeCompleted.
//
// Completion is synchronous, unlike Redact/Revise. There is no reopen operation
// or stream-status RPC in the pinned contract. It carries no actor/reason fields
// on the wire: consumers must authorize and audit it externally. No retries,
// local append notifications or unit-of-work staging occur. Transport failures
// are MutationOutcomeUnknownError, not proof the stream remains open.
func (s *Sequence) CompleteStream(ctx context.Context, streamType events.StreamType, streamID events.StreamID) (events.SequenceNumber, error) {
	if err := ctx.Err(); err != nil {
		return events.Unavailable, err
	}
	if strings.TrimSpace(string(streamType)) == "" || strings.TrimSpace(string(streamID)) == "" {
		return events.Unavailable, fmt.Errorf("%w: explicit stream type and ID are required", faults.ErrInvalidConfiguration)
	}
	response, err := s.service.CompleteStream(ctx, &sequences.CompleteStreamRequest{
		EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id),
		EventStreamType: string(streamType), EventStreamId: string(streamID),
	})
	if err := mutationOutcome(response, err); err != nil {
		return events.Unavailable, err
	}
	if err := wire.RequireMessage(response, "Response"); err != nil {
		return events.Unavailable, &MutationOutcomeUnknownError{Cause: err}
	}
	result := response.Response
	if result.IsSuccess && result.Error == sequences.CompleteStreamError_COMPLETE_STREAM_ERROR_None {
		return events.SequenceNumber(result.SequenceNumber), nil
	}
	if !result.IsSuccess {
		switch result.Error {
		case sequences.CompleteStreamError_AlreadyCompleted:
			return events.Unavailable, StreamAlreadyCompleted
		case sequences.CompleteStreamError_DefaultStreamCannotBeCompleted:
			return events.Unavailable, DefaultStreamCannotBeCompleted
		}
	}
	return events.Unavailable, &MutationOutcomeUnknownError{Cause: fmt.Errorf("%w: inconsistent or unknown stream completion response", faults.ErrProtocol)}
}
