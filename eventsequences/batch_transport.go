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
	common := batch.request
	request := &sequences.AppendManyRequest{EventStore: common.EventStore, Namespace: common.Namespace, EventSequenceId: common.EventSequenceId, EventSourceId: string(source), CorrelationId: common.CorrelationId, Causation: common.Causation, CausedBy: common.CausedBy, ConcurrencyScope: common.ConcurrencyScopes[0].Scope, Occurred: &sequences.SerializableDateTimeOffset{}}
	if config.occurred != nil {
		request.Occurred.Value = wire.DateTimeOffset(*config.occurred)
	}
	// The legacy overload has one shared static/dynamic tag union, in type order.
	seen := make(map[string]bool)
	for _, event := range common.Events {
		request.Events = append(request.Events, &sequences.EventToAppend{EventType: event.EventType, Content: event.Content, Subject: event.Subject})
		for _, tag := range event.Tags {
			if !seen[tag] {
				seen[tag] = true
				request.Tags = append(request.Tags, tag)
			}
		}
	}
	ctx = metadata.WithCorrelation(ctx, wire.Correlation(common.CorrelationId))
	var response *sequences.CommandResult_AppendManyResponse
	var err error
	if !batch.hasNamedTags() {
		response, err = s.service.AppendMany(ctx, request)
	} else {
		named := &sequences.AppendManyWithNamedTagsRequest{EventStore: request.EventStore, Namespace: request.Namespace, EventSequenceId: request.EventSequenceId, EventSourceId: request.EventSourceId, CorrelationId: request.CorrelationId, Tags: request.Tags, Occurred: request.Occurred, Causation: request.Causation, CausedBy: request.CausedBy, ConcurrencyScope: request.ConcurrencyScope}
		for i, event := range request.Events {
			named.Events = append(named.Events, &sequences.EventToAppendWithNamedTags{EventType: event.EventType, Content: event.Content, Subject: event.Subject, NamedTags: batch.named[i]})
		}
		response, err = s.service.AppendManyWithNamedTags(ctx, named)
	}
	return s.finishBatch(origin, batch, response, err)
}

func (s *Sequence) dispatchBatch(ctx context.Context, batch preparedBatch, origin Origin) (BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	request := batch.request
	ctx = metadata.WithCorrelation(ctx, wire.Correlation(request.CorrelationId))
	var response *sequences.CommandResult_AppendManyResponse
	var err error
	if !batch.hasNamedTags() {
		response, err = s.service.AppendManyForEventSources(ctx, request)
	} else {
		named := &sequences.AppendManyForEventSourcesWithNamedTagsRequest{EventStore: request.EventStore, Namespace: request.Namespace, EventSequenceId: request.EventSequenceId, CorrelationId: request.CorrelationId, Tags: request.Tags, Causation: request.Causation, CausedBy: request.CausedBy, ConcurrencyScopes: request.ConcurrencyScopes}
		for i, event := range request.Events {
			named.Events = append(named.Events, &sequences.EventForEventSourceIdWithNamedTags{EventSourceId: event.EventSourceId, EventSourceType: event.EventSourceType, EventStreamType: event.EventStreamType, EventStreamId: event.EventStreamId, EventType: event.EventType, Content: event.Content, Tags: event.Tags, NamedTags: batch.named[i], Occurred: event.Occurred, Subject: event.Subject, Causation: event.Causation})
		}
		response, err = s.service.AppendManyForEventSourcesWithNamedTags(ctx, named)
	}
	return s.finishBatch(origin, batch, response, err)
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
		var rejected *wire.EnvelopeError
		if errors.As(err, &rejected) && len(rejected.ExceptionMessages) == 0 {
			if len(batch.refs) == 0 {
				for _, validation := range rejected.ValidationResults {
					if strings.Contains(validation.Message, "At least one event is required") {
						err = errors.Join(fmt.Errorf("%w: kernel does not support eventless scope validation", faults.ErrUnsupported), err)
					}
				}
			}
			return BatchResult{Disposition: Rejected, CorrelationID: wire.Correlation(envelope.CorrelationId)}, err
		}
		return BatchResult{}, &OutcomeUnknownError{Cause: err}
	}
	if err = wire.RequireMessage(envelope, "Response"); err != nil {
		return BatchResult{}, &OutcomeUnknownError{Cause: err}
	}
	result, err := s.batchResult(envelope.Response, batch.refs)
	if err != nil {
		return BatchResult{}, &OutcomeUnknownError{Cause: err}
	}
	// The kernel's flag means ALL scopes were checked, not ANY. A batch mixing
	// protected and deliberately unchecked scopes legitimately reports false.
	allProtected := len(batch.request.ConcurrencyScopes) > 0
	for _, scope := range batch.request.ConcurrencyScopes {
		allProtected = allProtected && protectedScope(scope.Scope)
	}
	if allProtected && result.Disposition == Committed && !result.ConcurrencyCheckPerformed {
		return result, fmt.Errorf("%w: kernel committed without the requested concurrency checks; do not retry", faults.ErrUnsupported)
	}
	if result.Disposition == Unknown {
		return result, result.Err()
	}
	return result, nil
}
