// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

// Sequence is a concurrency-safe handle with immutable coordinates and local
// subscriptions. It must not be copied. Obtain it from EventStore; it does not
// own the client and cannot outlive the client's Close.
type Sequence struct {
	store                metadata.StoreName
	namespace            metadata.Namespace
	id                   events.SequenceID
	catalog              *events.Catalog
	service              sequences.EventSequencesClient
	concurrency          ConcurrencyPolicy
	decisions            decision.Provider
	appends              appendSubscriptions
	appendOriginResolver AppendOriginResolver
}

// New creates a low-level sequence over a caller-owned connection and registered
// catalog. Each call creates independent, handle-local append subscriptions; this
// is the low-level escape hatch from EventStore's shared sequence handles. It does
// not ensure registration or compatibility; prefer EventStore's handles, which
// enforce those barriers. It snapshots optional ConcurrencyPolicyProvider and
// AppendOriginResolverProvider configuration once from conn, without invoking
// the resolver. Invalid coordinates or nil dependencies fail.
func New(store metadata.StoreName, namespace metadata.Namespace, id events.SequenceID, catalog *events.Catalog, conn grpc.ClientConnInterface) (*Sequence, error) {
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || strings.TrimSpace(string(id)) == "" || catalog == nil || conn == nil {
		return nil, fmt.Errorf("%w: sequence coordinates, catalog and connection are required", faults.ErrInvalidConfiguration)
	}
	sequence := &Sequence{store: store, namespace: namespace, id: id, catalog: catalog, service: sequences.NewEventSequencesClient(conn)}
	sequence.decisions, _ = conn.(decision.Provider)
	if provider, ok := conn.(ConcurrencyPolicyProvider); ok {
		sequence.concurrency = provider.ConcurrencyPolicy()
	}
	if provider, ok := conn.(AppendOriginResolverProvider); ok {
		sequence.appendOriginResolver = provider.AppendOriginResolver()
	}
	return sequence, nil
}

// ID returns the selected event sequence.
func (s *Sequence) ID() events.SequenceID { return s.id }

// Append serializes and persists one registered event. Default concurrency queries
// the source/route tail; an empty tail is unchecked unless the client opts into
// first-append protection. Use NoMatchingEvent to protect
// first append, or Exact to protect an earlier read. Known domain rejections are
// results, not operation errors. Any post-dispatch failure is conservatively
// OutcomeUnknownError. The SDK never retries writes. Caller-owned event data must
// not be mutated concurrently with this call.
func (s *Sequence) Append(ctx context.Context, source events.SourceID, event any, options ...AppendOption) (result AppendResult, err error) {
	if err := ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	origin, err := s.resolveAppendOrigin(ctx)
	if err != nil {
		return AppendResult{Disposition: Rejected}, err
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
	if config.subject == nil {
		if subject, ok := descriptor.ResolveSubject(event); ok {
			config.subject = &subject
		}
	}
	request, err := s.request(ctx, source, descriptor, string(content), config)
	if err != nil {
		return AppendResult{}, err
	}
	if err = ctx.Err(); err != nil {
		return AppendResult{}, err
	}
	ctx = metadata.WithCorrelation(ctx, wire.Correlation(request.CorrelationId))
	dispatched := true
	defer func() {
		if dispatched {
			err = joinNotificationError(err, s.notifySingle(origin, source, descriptor.Ref(), config.route, wire.Correlation(request.CorrelationId), result, err))
		}
	}()
	var envelope *sequences.CommandResult_AppendResponse
	if len(config.named) == 0 {
		envelope, err = s.service.Append(ctx, request)
	} else {
		envelope, err = s.service.AppendWithNamedTags(ctx, namedRequest(request, config.named))
	}
	if err != nil {
		var local *faults.BeforeDispatch
		if errors.As(err, &local) {
			dispatched = false
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
	result, err = s.result(envelope.Response, descriptor.Ref())
	if err != nil {
		return AppendResult{}, &OutcomeUnknownError{Cause: err}
	}
	protected := request.ConcurrencyScope.ExpectsNoMatchingEvent || request.ConcurrencyScope.SequenceNumber != uint64(events.Unavailable)
	if protected && result.Disposition == Committed && !result.ConcurrencyCheckPerformed {
		return result, fmt.Errorf("%w: kernel committed without the requested concurrency check; do not retry", faults.ErrUnsupported)
	}
	if result.Disposition == Unknown {
		return result, result.Err()
	}
	return result, nil
}

func (s *Sequence) request(ctx context.Context, source events.SourceID, descriptor events.Descriptor, content string, config appendConfig) (*sequences.AppendRequest, error) {
	config.route = normalizedRoute(config.route)
	if err := validateAppendMetadata(config.subject, config.occurred, config.named); err != nil {
		return nil, err
	}
	subject := string(source)
	if config.subject != nil {
		subject = string(*config.subject)
	}
	if err := compliance.ValidateSubject(subject); err != nil {
		return nil, err
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
