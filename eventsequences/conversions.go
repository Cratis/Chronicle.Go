// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"fmt"
	"maps"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/observation"
)

func identityContract(identity identities.Identity) *sequences.Identity {
	result := &sequences.Identity{Subject: identity.Subject, Name: identity.Name, UserName: identity.UserName}
	if identity.OnBehalfOf != nil {
		result.OnBehalfOf = identityContract(*identity.OnBehalfOf)
	}
	return result
}
func causationContract(chain []metadata.Causation) []*sequences.Causation {
	result := make([]*sequences.Causation, len(chain))
	for i, cause := range chain {
		result[i] = &sequences.Causation{Occurred: &sequences.SerializableDateTimeOffset{Value: wire.DateTimeOffset(cause.Occurred)}, Type: cause.Type, Properties: maps.Clone(cause.Properties)}
	}
	return result
}
func namedRequest(request *sequences.AppendRequest, tags []events.NamedTag) *sequences.AppendWithNamedTagsRequest {
	result := &sequences.AppendWithNamedTagsRequest{EventStore: request.EventStore, Namespace: request.Namespace, EventSequenceId: request.EventSequenceId,
		EventSourceId: request.EventSourceId, EventSourceType: request.EventSourceType, EventStreamType: request.EventStreamType, EventStreamId: request.EventStreamId,
		EventType: request.EventType, Content: request.Content, CorrelationId: request.CorrelationId, Tags: request.Tags, Occurred: request.Occurred,
		Subject: request.Subject, Causation: request.Causation, CausedBy: request.CausedBy, ConcurrencyScope: request.ConcurrencyScope}
	seen := make(map[events.NamedTag]bool)
	for _, tag := range tags {
		if !seen[tag] {
			seen[tag] = true
			result.NamedTags = append(result.NamedTags, &sequences.NamedTag{Name: tag.Name, Value: tag.Value})
		}
	}
	return result
}

func (s *Sequence) result(response *sequences.AppendResponse, eventType events.TypeRef) (AppendResult, error) {
	if response == nil {
		return AppendResult{}, faults.ErrProtocol
	}
	constraintsPresent, concurrencyPresent, errorsPresent := len(response.ConstraintViolations) > 0, response.ConcurrencyViolation != nil, len(response.Errors) > 0
	if response.HasConstraintViolations != constraintsPresent || response.HasConcurrencyViolations != concurrencyPresent || response.HasErrors != errorsPresent || response.IsSuccess == (constraintsPresent || concurrencyPresent || errorsPresent) {
		return AppendResult{}, fmt.Errorf("%w: inconsistent append result flags", faults.ErrProtocol)
	}
	result := AppendResult{Disposition: Unknown, CorrelationID: wire.Correlation(response.CorrelationId), ConcurrencyCheckPerformed: response.ConcurrencyCheckPerformed}
	for _, violation := range response.ConstraintViolations {
		if violation == nil {
			return AppendResult{}, faults.ErrProtocol
		}
		result.ConstraintViolations = append(result.ConstraintViolations, constraints.Violation{EventTypeID: events.TypeID(violation.EventTypeId), SequenceNumber: events.SequenceNumber(violation.SequenceNumber),
			Type: constraints.Type(violation.ConstraintType), ConstraintName: violation.ConstraintName, Message: violation.Message, Details: maps.Clone(violation.Details)})
	}
	if violation := response.ConcurrencyViolation; violation != nil {
		result.ConcurrencyViolations = []ConcurrencyViolation{{SourceID: events.SourceID(violation.EventSourceId), Expected: events.SequenceNumber(violation.ExpectedSequenceNumber), Actual: events.SequenceNumber(violation.ActualSequenceNumber)}}
	}
	for _, err := range response.Errors {
		result.Errors = append(result.Errors, AppendError(err))
	}
	if constraintsPresent || concurrencyPresent {
		result.Disposition = Rejected
	}
	if response.IsSuccess {
		position := events.SequenceNumber(response.SequenceNumber)
		if position >= events.Unavailable-2 {
			return AppendResult{}, fmt.Errorf("%w: reserved committed position", faults.ErrProtocol)
		}
		result.Disposition, result.Position = Committed, &position
		result.Target = observation.CompletionTarget{Store: s.store, Namespace: s.namespace, Sequence: s.id, First: copyPointer(&position), EventTypeTails: map[events.TypeID]events.SequenceNumber{eventType.ID: position}}
	}
	return result, nil
}
