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
	"github.com/cratis/chronicle.go/jobs"
)

// Copy starts a durable kernel job that appends copies of the events of source
// to target in this sequence, and returns the job's ID once the kernel has
// started it. Follow the job through the jobs service. With no eventTypes every
// type is copied; supplied IDs must be registered and select all generations.
// Source and target must be nonblank.
//
// The copies are new events with new sequence numbers, observed like any other
// append; the source events are untouched. The kernel selects the matching
// events when the job runs, not at call time, and resumes the job after a
// restart without appending a copy twice. A refused append, such as a unique
// constraint violation, fails the job, and copies appended before it stay.
// Copying is an administrative operation: authorize it at the consumer boundary.
//
// Like the other history operations it never retries, emits AppendOperations,
// notifies local append observers or participates in a unit of work. Transport
// failures and a response without a job ID are MutationOutcomeUnknownError: a job
// may have started, so do not blindly call Copy again. This operation needs a
// kernel that provides CopyEventsForEventSource.
func (s *Sequence) Copy(ctx context.Context, source, target events.SourceID, eventTypes ...events.TypeID) (jobs.ID, error) {
	if err := ctx.Err(); err != nil {
		return jobs.ID{}, err
	}
	if strings.TrimSpace(string(source)) == "" || strings.TrimSpace(string(target)) == "" {
		return jobs.ID{}, fmt.Errorf("%w: source and target are required", faults.ErrInvalidConfiguration)
	}
	ids, err := s.registeredTypeIDs("copy", eventTypes)
	if err != nil {
		return jobs.ID{}, err
	}
	response, err := s.service.CopyEventsForEventSource(ctx, &sequences.CopyEventsForEventSourceRequest{
		EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id),
		SourceId: string(source), TargetId: string(target), EventTypes: ids,
	})
	if err := mutationOutcome(response, err); err != nil {
		return jobs.ID{}, err
	}
	if err := wire.RequireMessage(response, "Response"); err != nil {
		return jobs.ID{}, &MutationOutcomeUnknownError{Cause: err}
	}
	id := jobs.ID(wire.Correlation(response.Response))
	if id == (jobs.ID{}) {
		return jobs.ID{}, &MutationOutcomeUnknownError{Cause: fmt.Errorf("%w: missing copy job ID", faults.ErrProtocol)}
	}
	return id, nil
}
