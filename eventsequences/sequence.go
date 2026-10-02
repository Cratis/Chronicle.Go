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
	"google.golang.org/grpc"
)

// Sequence is an immutable concurrency-safe handle. Obtain it from EventStore;
// it does not own the client and cannot outlive the client's Close.
type Sequence struct {
	store     metadata.StoreName
	namespace metadata.Namespace
	id        events.SequenceID
	catalog   *events.Catalog
	service   sequences.EventSequencesClient
}

// New creates a low-level sequence over a caller-owned connection and registered
// catalog. It does not ensure registration or compatibility; prefer EventStore's
// handles, which enforce those barriers. Invalid coordinates or nil dependencies fail.
func New(store metadata.StoreName, namespace metadata.Namespace, id events.SequenceID, catalog *events.Catalog, conn grpc.ClientConnInterface) (*Sequence, error) {
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || strings.TrimSpace(string(id)) == "" || catalog == nil || conn == nil {
		return nil, fmt.Errorf("%w: sequence coordinates, catalog and connection are required", faults.ErrInvalidConfiguration)
	}
	return &Sequence{store: store, namespace: namespace, id: id, catalog: catalog, service: sequences.NewEventSequencesClient(conn)}, nil
}

// ID returns the selected event sequence.
func (s *Sequence) ID() events.SequenceID { return s.id }

// Append serializes and persists one registered event. Default concurrency queries
// the source/route tail; an empty tail is unchecked. Use NoMatchingEvent to protect
// first append, or Exact to protect an earlier read. Known domain rejections are
// results, not operation errors. Any post-dispatch failure is conservatively
// OutcomeUnknownError. The SDK never retries writes. Caller-owned event data must
// not be mutated concurrently with this call.
func (s *Sequence) Append(ctx context.Context, source events.SourceID, event any, options ...AppendOption) (AppendResult, error) {
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	if strings.TrimSpace(string(source)) == "" || s.id == "" {
		return AppendResult{}, fmt.Errorf("%w: source and sequence are required", faults.ErrInvalidConfiguration)
	}
	descriptor, found := s.catalog.Lookup(event)
	if !found {
		return AppendResult{}, faults.ErrNotRegistered
	}
	content, err := descriptor.Marshal(event)
	if err != nil {
		return AppendResult{}, err
	}
	config := appendConfig{correlation: metadata.Correlation(ctx)}
	for _, option := range options {
		if option == nil {
			return AppendResult{}, fmt.Errorf("%w: nil append option", faults.ErrInvalidConfiguration)
		}
		option(&config)
	}
	request, err := s.request(ctx, source, descriptor, string(content), config)
	if err != nil {
		return AppendResult{}, err
	}
	if err = ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	ctx = metadata.WithCorrelation(ctx, wire.Correlation(request.CorrelationId))
	var envelope *sequences.CommandResult_AppendResponse
	if len(config.named) == 0 {
		envelope, err = s.service.Append(ctx, request)
	} else {
		envelope, err = s.service.AppendWithNamedTags(ctx, namedRequest(request, config.named))
	}
	if err != nil {
		var local *faults.BeforeDispatch
		if errors.As(err, &local) {
			return AppendResult{}, local.Cause
		}
		return AppendResult{}, &OutcomeUnknownError{Cause: err}
	}
	if err = wire.CheckEnvelope(envelope); err != nil {
		var rejected *wire.EnvelopeError
		if errors.As(err, &rejected) && len(rejected.ExceptionMessages) == 0 {
			return AppendResult{Disposition: Rejected, CorrelationID: wire.Correlation(envelope.CorrelationId)}, err
		}
		return AppendResult{}, &OutcomeUnknownError{Cause: err}
	}
	if err = wire.RequireMessage(envelope, "Response"); err != nil {
		return AppendResult{}, &OutcomeUnknownError{Cause: err}
	}
	result, err := s.result(envelope.Response, descriptor.Ref())
	if err != nil {
		return AppendResult{}, &OutcomeUnknownError{Cause: err}
	}
	protected := request.ConcurrencyScope.ExpectsNoMatchingEvent || request.ConcurrencyScope.SequenceNumber != uint64(events.Unavailable)
	if protected && result.Disposition == Committed && !result.ConcurrencyCheckPerformed {
		return result, fmt.Errorf("%w: kernel committed without the requested concurrency check; do not retry", faults.ErrUnsupported)
	}
	return result, nil
}

func (s *Sequence) request(ctx context.Context, source events.SourceID, descriptor events.Descriptor, content string, config appendConfig) (*sequences.AppendRequest, error) {
	if config.route.SourceType == "" {
		config.route.SourceType = events.DefaultSourceType
	}
	if config.route.StreamType == "" {
		config.route.StreamType = events.AllStreamTypes
	}
	if config.route.StreamID == "" {
		config.route.StreamID = events.DefaultStreamID
	}
	if config.subject != nil && *config.subject == "" {
		return nil, fmt.Errorf("%w: empty compliance subject", faults.ErrInvalidConfiguration)
	}
	for _, tag := range config.named {
		if strings.TrimSpace(tag.Name) == "" {
			return nil, fmt.Errorf("%w: empty named tag name", faults.ErrInvalidConfiguration)
		}
	}
	if config.occurred != nil && (config.occurred.Year() < 1 || config.occurred.Year() > 9999) {
		return nil, fmt.Errorf("%w: occurrence outside .NET date range", faults.ErrInvalidConfiguration)
	}
	if config.correlation == (metadata.CorrelationID{}) {
		var err error
		config.correlation, err = metadata.NewCorrelationID()
		if err != nil {
			return nil, err
		}
	}
	scope, err := s.resolveScope(ctx, source, config)
	if err != nil {
		return nil, err
	}
	subject := string(source)
	if config.subject != nil {
		subject = string(*config.subject)
	}
	ref := descriptor.Ref()
	request := &sequences.AppendRequest{EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id), EventSourceId: string(source),
		EventSourceType: string(config.route.SourceType), EventStreamType: string(config.route.StreamType), EventStreamId: string(config.route.StreamID),
		EventType: &sequences.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)}, Content: content,
		CorrelationId: wire.Guid(config.correlation), Subject: subject, ConcurrencyScope: scope, Occurred: &sequences.SerializableDateTimeOffset{},
		CausedBy: identityContract(metadata.Identity(ctx)), Causation: causationContract(metadata.CausationChain(ctx)),
	}
	if config.occurred != nil {
		request.Occurred.Value = wire.DateTimeOffset(*config.occurred)
	}
	seen := make(map[events.Tag]bool)
	for _, tag := range append(descriptor.Tags(), config.tags...) {
		if !seen[tag] {
			seen[tag] = true
			request.Tags = append(request.Tags, string(tag))
		}
	}
	return request, nil
}
