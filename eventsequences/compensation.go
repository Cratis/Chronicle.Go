// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"fmt"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

// Kernel rejection codes of a compensation. They are domain rejections reported
// on the result, not operation errors.
const (
	// CompensatedEventNotFound means the event being compensated for is not in the sequence.
	CompensatedEventNotFound AppendError = "CompensatedEventNotFound"
	// EventTypeDoesNotCompensateEventType means the compensating event's type is not
	// declared as a compensation for the type of the event being compensated.
	EventTypeDoesNotCompensateEventType AppendError = "EventTypeDoesNotCompensateEventType"
)

// Compensate appends a compensating event for the event at position. The original
// is never changed; the new event records position as its Context.CompensationFor.
// The event source, stream and subject come from the original, so WithRoute,
// WithScope, WithSubject and WithNamedTags are refused with ErrInvalidConfiguration
// rather than silently ignored; WithCorrelation, WithTags and WithOccurred apply.
// The event's registered type must be declared with events.WithCompensationFor;
// otherwise it is refused before dispatch. The kernel checks that the target exists
// and that the type compensates the target's type: those rejections are returned as
// a Rejected result carrying CompensatedEventNotFound or
// EventTypeDoesNotCompensateEventType, with a nil error (promote with Err). Zero is
// a valid target; the three highest positions are reserved and invalid.
// Chronicle 19.125.0 or later is required; an older kernel fails the call. Like
// Revise it takes no retries, local append notifications or unit-of-work staging.
// A post-dispatch failure is an OutcomeUnknownError: never blindly retry, a retry
// can add a second compensation.
func (s *Sequence) Compensate(ctx context.Context, position events.SequenceNumber, event any, options ...AppendOption) (AppendResult, error) {
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	if position >= events.Unavailable-2 {
		return AppendResult{}, fmt.Errorf("%w: an event position is required", faults.ErrInvalidConfiguration)
	}
	if s.id == "" {
		return AppendResult{}, fmt.Errorf("%w: sequence is required", faults.ErrInvalidConfiguration)
	}
	config := appendConfig{}
	for _, option := range options {
		if option == nil {
			return AppendResult{}, fmt.Errorf("%w: nil append option", faults.ErrInvalidConfiguration)
		}
		option(&config)
	}
	if config.route != (Route{}) || config.scope != nil || config.subject != nil || len(config.named) != 0 {
		return AppendResult{}, fmt.Errorf("%w: a compensation takes its source, stream and subject from the compensated event and has no scope or named tags", faults.ErrInvalidConfiguration)
	}
	if err := validateAppendMetadata(nil, config.occurred, nil); err != nil {
		return AppendResult{}, err
	}
	descriptor, ok := s.catalog.Lookup(event)
	if !ok {
		return AppendResult{}, faults.ErrNotRegistered
	}
	if _, ok := descriptor.CompensationFor(); !ok {
		return AppendResult{}, fmt.Errorf("%w: the event type is not declared as a compensation", faults.ErrInvalidConfiguration)
	}
	audit, err := s.outgoing.Resolve(ctx, config.correlation, config.correlationSet, metadata.CorrelationID{})
	if err != nil {
		return AppendResult{}, err
	}
	ctx = audit.Context(ctx)
	content, err := s.outgoing.Encode(ctx, descriptor, event, false, 0)
	if err != nil {
		return AppendResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	ref := descriptor.Ref()
	request := &sequences.CompensateRequest{
		EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id),
		CompensationFor: uint64(position), EventType: &sequences.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)},
		Content: string(content), CorrelationId: wire.Guid(audit.Correlation), Occurred: &sequences.SerializableDateTimeOffset{},
		Causation: causationContract(audit.Causes), CausedBy: identityContract(audit.Identity), Tags: compensationTags(descriptor, config.tags),
	}
	if config.occurred != nil {
		request.Occurred.Value = wire.DateTimeOffset(*config.occurred)
	}
	envelope, err := s.service.Compensate(ctx, request)
	result, _, err := s.envelopeResult(envelope, err, ref)
	if err != nil {
		return result, err
	}
	if result.Disposition == Unknown && len(result.Errors) > 0 && allKnownCompensationRejections(result.Errors) {
		result.Disposition = Rejected
	}
	if result.Disposition == Unknown {
		return result, result.Err()
	}
	return result, nil
}

// compensationTags merges static and dynamic tags distinctly, static first.
func compensationTags(descriptor events.Descriptor, dynamic []events.Tag) []string {
	var tags []string
	seen := make(map[events.Tag]bool)
	for _, tag := range append(descriptor.Tags(), dynamic...) {
		if !seen[tag] {
			seen[tag] = true
			tags = append(tags, string(tag))
		}
	}
	return tags
}

func allKnownCompensationRejections(errs []AppendError) bool {
	for _, err := range errs {
		if err != CompensatedEventNotFound && err != EventTypeDoesNotCompensateEventType {
			return false
		}
	}
	return true
}
