// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

func (s *Sequence) dispatchMany(ctx context.Context, source events.SourceID, config appendConfig, batch preparedBatch, origin Origin) (BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	request := batch.legacyRequest(source, config)
	ctx = metadata.WithCorrelation(ctx, wire.Correlation(batch.request.CorrelationId))
	response, err := s.sendLegacyMany(ctx, request, batch)
	return s.finishBatch(origin, batch, response, err)
}

func (b preparedBatch) legacyRequest(source events.SourceID, config appendConfig) *sequences.AppendManyRequest {
	common := b.request
	request := &sequences.AppendManyRequest{EventStore: common.EventStore, Namespace: common.Namespace, EventSequenceId: common.EventSequenceId, EventSourceId: string(source), CorrelationId: common.CorrelationId, Causation: common.Causation, CausedBy: common.CausedBy, ConcurrencyScope: common.ConcurrencyScopes[0].Scope, Occurred: &sequences.SerializableDateTimeOffset{}}
	if config.occurred != nil {
		request.Occurred.Value = wire.DateTimeOffset(*config.occurred)
	}
	appendLegacyEvents(request, common.Events)
	return request
}

func appendLegacyEvents(request *sequences.AppendManyRequest, events []*sequences.EventForEventSourceId) {
	// The legacy overload has one shared static/dynamic tag union, in type order.
	seen := make(map[string]bool)
	for _, event := range events {
		request.Events = append(request.Events, &sequences.EventToAppend{EventType: event.EventType, Content: event.Content, Subject: event.Subject})
		for _, tag := range event.Tags {
			if !seen[tag] {
				seen[tag] = true
				request.Tags = append(request.Tags, tag)
			}
		}
	}
}

func (s *Sequence) sendLegacyMany(ctx context.Context, request *sequences.AppendManyRequest, batch preparedBatch) (*sequences.CommandResult_AppendManyResponse, error) {
	if !batch.hasNamedTags() {
		return s.service.AppendMany(ctx, request)
	}
	named := &sequences.AppendManyWithNamedTagsRequest{EventStore: request.EventStore, Namespace: request.Namespace, EventSequenceId: request.EventSequenceId, EventSourceId: request.EventSourceId, CorrelationId: request.CorrelationId, Tags: request.Tags, Occurred: request.Occurred, Causation: request.Causation, CausedBy: request.CausedBy, ConcurrencyScope: request.ConcurrencyScope}
	for i, event := range request.Events {
		named.Events = append(named.Events, &sequences.EventToAppendWithNamedTags{EventType: event.EventType, Content: event.Content, Subject: event.Subject, NamedTags: batch.named[i]})
	}
	return s.service.AppendManyWithNamedTags(ctx, named)
}

func (s *Sequence) dispatchBatch(ctx context.Context, batch preparedBatch, origin Origin) (BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	response, err := sendBatch(ctx, batch, s.service)
	return s.finishBatch(origin, batch, response, err)
}

func sendBatch(ctx context.Context, batch preparedBatch, service sequences.EventSequencesClient) (*sequences.CommandResult_AppendManyResponse, error) {
	request := batch.request
	ctx = metadata.WithCorrelation(ctx, wire.Correlation(request.CorrelationId))
	var response *sequences.CommandResult_AppendManyResponse
	var err error
	if !batch.hasNamedTags() {
		response, err = service.AppendManyForEventSources(ctx, request)
	} else {
		named := &sequences.AppendManyForEventSourcesWithNamedTagsRequest{EventStore: request.EventStore, Namespace: request.Namespace, EventSequenceId: request.EventSequenceId, CorrelationId: request.CorrelationId, Tags: request.Tags, Causation: request.Causation, CausedBy: request.CausedBy, ConcurrencyScopes: request.ConcurrencyScopes}
		for i, event := range request.Events {
			named.Events = append(named.Events, &sequences.EventForEventSourceIdWithNamedTags{EventSourceId: event.EventSourceId, EventSourceType: event.EventSourceType, EventStreamType: event.EventStreamType, EventStreamId: event.EventStreamId, EventType: event.EventType, Content: event.Content, Tags: event.Tags, NamedTags: batch.named[i], Occurred: event.Occurred, Subject: event.Subject, Causation: event.Causation})
		}
		response, err = service.AppendManyForEventSourcesWithNamedTags(ctx, named)
	}
	return response, err
}

func (b preparedBatch) hasNamedTags() bool {
	for _, tags := range b.named {
		if len(tags) > 0 {
			return true
		}
	}
	return false
}

func (s *Sequence) finishBatch(origin Origin, batch preparedBatch, envelope *sequences.CommandResult_AppendManyResponse, err error) (BatchResult, error) {
	var local *faults.BeforeDispatch
	if errors.As(err, &local) {
		return BatchResult{}, local.Cause
	}
	result, err := s.batchOutcome(batch, envelope, err)
	return result, joinNotificationError(err, s.notifyBatch(origin, batch, result, err))
}

func (s *Sequence) batchOutcome(batch preparedBatch, envelope *sequences.CommandResult_AppendManyResponse, err error) (BatchResult, error) {
	if err != nil {
		var local *faults.BeforeDispatch
		if errors.As(err, &local) {
			return BatchResult{}, local.Cause
		}
		return BatchResult{}, &OutcomeUnknownError{Cause: err}
	}
	if err = wire.CheckEnvelope(envelope); err != nil {
		return batch.rejectedOutcome(envelope, err)
	}
	if err = wire.RequireMessage(envelope, "Response"); err != nil {
		return BatchResult{}, &OutcomeUnknownError{Cause: err}
	}
	result, err := s.batchResult(envelope.Response, batch.refs)
	if err != nil {
		return BatchResult{}, &OutcomeUnknownError{Cause: err}
	}
	if batch.missingConcurrencyCheck(result) {
		return result, fmt.Errorf("%w: kernel committed without the requested concurrency checks; do not retry", faults.ErrUnsupported)
	}
	if result.Disposition == Unknown {
		return result, result.Err()
	}
	return result, nil
}

func (b preparedBatch) rejectedOutcome(envelope *sequences.CommandResult_AppendManyResponse, err error) (BatchResult, error) {
	var rejected *wire.EnvelopeError
	if !errors.As(err, &rejected) || len(rejected.ExceptionMessages) != 0 {
		return BatchResult{}, &OutcomeUnknownError{Cause: err}
	}
	if len(b.refs) == 0 {
		err = eventlessValidationError(rejected, err)
	}
	return BatchResult{Disposition: Rejected, CorrelationID: wire.Correlation(envelope.CorrelationId)}, err
}

func eventlessValidationError(rejected *wire.EnvelopeError, err error) error {
	for _, validation := range rejected.ValidationResults {
		if strings.Contains(validation.Message, "At least one event is required") {
			err = errors.Join(fmt.Errorf("%w: kernel does not support eventless scope validation", faults.ErrUnsupported), err)
		}
	}
	return err
}

func (b preparedBatch) missingConcurrencyCheck(result BatchResult) bool {
	return b.allScopesProtected() && result.Disposition == Committed && !result.ConcurrencyCheckPerformed
}

func (b preparedBatch) allScopesProtected() bool {
	// The kernel's flag means ALL scopes were checked, not ANY. A batch mixing
	// protected and deliberately unchecked scopes legitimately reports false.
	allProtected := len(b.request.ConcurrencyScopes) > 0
	for _, scope := range b.request.ConcurrencyScopes {
		allProtected = allProtected && protectedScope(scope.Scope)
	}
	return allProtected
}
