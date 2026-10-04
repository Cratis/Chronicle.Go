// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/outgoing"
	"github.com/cratis/chronicle.go/internal/preparation"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

// AppendMany atomically appends registered values for one source, in input order.
// Static tags from every type are unioned across the whole batch, like C#. The
// legacy RPC is used only for the default route; other routes use the ordered
// heterogeneous RPC. Options and failure semantics are the same as Append.
// Empty values require an effective protected scope, not an unchecked no-op.
func (s *Sequence) AppendMany(ctx context.Context, source events.SourceID, values []any, options ...AppendOption) (BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	if strings.TrimSpace(string(source)) == "" {
		return BatchResult{}, faults.ErrInvalidConfiguration
	}
	config := appendConfig{correlation: metadata.Correlation(ctx)}
	for _, option := range options {
		if option == nil {
			return BatchResult{}, fmt.Errorf("%w: nil append option", faults.ErrInvalidConfiguration)
		}
		option(&config)
	}
	if err := validateAppendMetadata(config.subject, config.occurred, config.named); err != nil {
		return BatchResult{}, err
	}
	config.route = normalizedRoute(config.route)
	scope := defaultScope(source, config.route)
	if config.scope != nil {
		scope = *config.scope
	}
	entries := make([]Entry, len(values))
	var tags []events.Tag
	for i, value := range values {
		descriptor, found := s.catalog.Lookup(value)
		if !found {
			return BatchResult{}, faults.ErrNotRegistered
		}
		tags = append(tags, descriptor.Tags()...)
		entries[i] = Entry{Source: source, Event: value, Route: config.route, Occurred: config.occurred, Subject: config.subject}
	}
	tags = append(tags, config.tags...)
	// Validate and serialize every event before invoking a user strategy or I/O.
	batch, err := s.snapshotBatch(ctx, entries, batchConfig{correlation: config.correlation, correlationSet: config.correlationSet, tags: tags, named: config.named, scopes: []LabeledScope{{Label: string(source), Scope: scope}}})
	if err != nil {
		return BatchResult{}, err
	}
	origin, err := s.resolveAppendOrigin(ctx)
	if err != nil {
		return BatchResult{Disposition: Rejected}, err
	}
	ctx = batch.audit.Context(ctx)
	if config.scope == nil {
		scope, err = s.automaticScope(ctx, source, config.route)
		if err != nil {
			return BatchResult{}, err
		}
	}
	batch, err = s.resolveBatch(ctx, batch, []LabeledScope{{Label: string(source), Scope: scope}})
	if err != nil {
		return BatchResult{}, err
	}
	if config.route == normalizedRoute(Route{}) {
		return s.dispatchMany(ctx, source, config, batch, origin)
	}
	return s.dispatchBatch(ctx, batch, origin)
}

// AppendBatch atomically appends entries without grouping or reordering sources.
// All values/metadata are serialized before any scope read or write. Missing
// source checks resolve optimistically using each source's first entry route.
// An empty batch must carry a protected check. Transport failures are unknown
// outcomes and never retried; known atomic rejections are returned as results.
func (s *Sequence) AppendBatch(ctx context.Context, entries []Entry, options ...BatchOption) (BatchResult, error) {
	if err := ctx.Err(); err != nil {
		return BatchResult{}, err
	}
	config := batchConfig{correlation: metadata.Correlation(ctx)}
	for _, option := range options {
		if option == nil {
			return BatchResult{}, fmt.Errorf("%w: nil batch option", faults.ErrInvalidConfiguration)
		}
		option(&config)
	}
	batch, err := s.snapshotBatch(ctx, entries, config)
	if err != nil {
		return BatchResult{}, err
	}
	origin, err := s.resolveAppendOrigin(ctx)
	if err != nil {
		return BatchResult{Disposition: Rejected}, err
	}
	ctx = batch.audit.Context(ctx)
	scopes, err := s.automaticBatchScopes(ctx, entries, config.scopes)
	if err != nil {
		return BatchResult{}, err
	}
	batch, err = s.resolveBatch(ctx, batch, scopes)
	if err != nil {
		return BatchResult{}, err
	}
	return s.dispatchBatch(ctx, batch, origin)
}

type preparedBatch struct {
	request *sequences.AppendManyForEventSourcesRequest
	named   [][]*sequences.NamedTag
	refs    []events.TypeRef
	audit   outgoing.Audit
}

func (s *Sequence) snapshotBatch(ctx context.Context, entries []Entry, config batchConfig) (preparedBatch, error) {
	entries, descriptors, err := s.preflightEntries(entries, config)
	if err != nil {
		return preparedBatch{}, err
	}
	audit, err := s.outgoing.Resolve(ctx, config.correlation, config.correlationSet, config.inheritedCorrelation)
	if err != nil {
		return preparedBatch{}, err
	}
	if config.bound != nil && (audit.Correlation != config.bound.Correlation || !reflect.DeepEqual(audit.Identity, config.bound.Identity)) {
		return preparedBatch{}, fmt.Errorf("%w: unit of work correlation and actor must remain fixed", faults.ErrInvalidConfiguration)
	}
	selected := audit.Context(ctx)
	for _, cause := range audit.Causes {
		if cause.Occurred.Year() < 1 || cause.Occurred.Year() > 9999 {
			return preparedBatch{}, faults.ErrInvalidConfiguration
		}
	}
	subjects := make([]events.Subject, len(entries))
	for i, entry := range entries {
		subject, err := frozenSubject(ctx, descriptors[i], entry.Event, entry.Source, entry.Subject, i)
		if err != nil {
			return preparedBatch{}, err
		}
		subjects[i] = subject
	}
	request := &sequences.AppendManyForEventSourcesRequest{EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id), CorrelationId: wire.Guid(audit.Correlation), Causation: causationContract(audit.Causes), CausedBy: identityContract(audit.Identity)}
	batch := preparedBatch{request: request, audit: audit}
	for i, entry := range entries {
		descriptor := descriptors[i]
		explicitSubject := entry.Subject != nil
		subject := subjects[i]
		entryCtx := metadata.WithCausationChain(selected, append(slices.Clone(audit.Causes), entry.Causation...))
		content, err := s.outgoing.Encode(entryCtx, descriptor, entry.Event, explicitSubject, i)
		if err != nil {
			return preparedBatch{}, err
		}
		options := appendConfig{route: entry.Route, scope: &Scope{Expectation: NoCheck()}, correlation: audit.Correlation, subject: &subject, occurred: entry.Occurred,
			tags: append(slices.Clone(entry.Tags), config.tags...), named: append(slices.Clone(entry.NamedTags), config.named...)}
		one, err := s.request(entryCtx, entry.Source, descriptor, string(content), options)
		if err != nil {
			return preparedBatch{}, err
		}
		event := &sequences.EventForEventSourceId{EventSourceId: one.EventSourceId, EventSourceType: one.EventSourceType, EventStreamType: one.EventStreamType, EventStreamId: one.EventStreamId, EventType: one.EventType, Content: one.Content, Tags: one.Tags, Subject: one.Subject, Occurred: one.Occurred}
		if len(entry.Causation) > 0 {
			event.Causation = one.Causation
		}
		request.Events = append(request.Events, event)
		batch.named = append(batch.named, namedRequest(one, options.named).NamedTags)
		batch.refs = append(batch.refs, descriptor.Ref())
	}
	if err := ctx.Err(); err != nil {
		return preparedBatch{}, err
	}
	return batch, nil
}

func frozenSubject(ctx context.Context, descriptor events.Descriptor, value any, source events.SourceID, explicit *events.Subject, index int) (events.Subject, error) {
	subject := events.Subject(source)
	if explicit != nil {
		subject = *explicit
	} else {
		err := preparation.Call(ctx, "subject", -1, index, func() error {
			if resolved, ok := descriptor.ResolveSubject(value); ok {
				subject = resolved
			}
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	if err := compliance.ValidateSubject(string(subject)); err != nil {
		return "", err
	}
	return subject, nil
}

func (s *Sequence) preflightEntries(entries []Entry, config batchConfig) ([]Entry, []events.Descriptor, error) {
	if err := validateAppendMetadata(nil, nil, config.named); err != nil {
		return nil, nil, err
	}
	if _, err := batchScopes(entries, config.scopes); err != nil {
		return nil, nil, err
	}
	entries = slices.Clone(entries)
	descriptors := make([]events.Descriptor, len(entries))
	for i := range entries {
		entry := &entries[i]
		if strings.TrimSpace(string(entry.Source)) == "" {
			return nil, nil, faults.ErrInvalidConfiguration
		}
		descriptor, found := s.catalog.Lookup(entry.Event)
		if !found {
			return nil, nil, faults.ErrNotRegistered
		}
		descriptors[i] = descriptor
		entry.Subject, entry.Occurred = copyPointer(entry.Subject), copyPointer(entry.Occurred)
		entry.Tags, entry.NamedTags = slices.Clone(entry.Tags), slices.Clone(entry.NamedTags)
		if err := validateAppendMetadata(entry.Subject, entry.Occurred, entry.NamedTags); err != nil {
			return nil, nil, err
		}
		entry.Causation = metadata.CausationChain(metadata.WithCausationChain(context.Background(), entry.Causation))
		for _, cause := range entry.Causation {
			if cause.Occurred.Year() < 1 || cause.Occurred.Year() > 9999 {
				return nil, nil, faults.ErrInvalidConfiguration
			}
		}
	}
	return entries, descriptors, nil
}

func (s *Sequence) resolveBatch(ctx context.Context, batch preparedBatch, scopes []LabeledScope) (preparedBatch, error) {
	request := batch.request
	for _, labeled := range scopes {
		scope, err := s.resolveLabeledScope(ctx, events.SourceID(labeled.Label), labeled.Scope)
		if err != nil {
			return preparedBatch{}, err
		}
		request.ConcurrencyScopes = append(request.ConcurrencyScopes, &sequences.EventSourceConcurrencyScope{EventSourceId: labeled.Label, Scope: scope})
	}
	if len(batch.refs) == 0 && !hasProtectedScope(request.ConcurrencyScopes) {
		return preparedBatch{}, fmt.Errorf("%w: an empty batch requires a protected scope", faults.ErrInvalidConfiguration)
	}
	return batch, nil
}

func batchScopes(entries []Entry, explicit []LabeledScope) ([]LabeledScope, error) {
	result := append([]LabeledScope(nil), explicit...)
	seen := make(map[string]bool)
	for _, labeled := range result {
		if strings.TrimSpace(labeled.Label) == "" || seen[labeled.Label] {
			return nil, fmt.Errorf("%w: blank or duplicate scope label", faults.ErrInvalidConfiguration)
		}
		seen[labeled.Label] = true
		if _, err := scopeContract(events.SourceID(labeled.Label), labeled.Scope); err != nil {
			return nil, err
		}
	}
	for _, entry := range entries {
		label := string(entry.Source)
		if !seen[label] {
			seen[label] = true
			result = append(result, LabeledScope{Label: label, Scope: defaultScope(entry.Source, entry.Route)})
		}
	}
	return result, nil
}

func protectedScope(scope *sequences.ConcurrencyScope) bool {
	return scope.ExpectsNoMatchingEvent || scope.SequenceNumber != uint64(events.Unavailable)
}

func hasProtectedScope(scopes []*sequences.EventSourceConcurrencyScope) bool {
	for _, scope := range scopes {
		if protectedScope(scope.Scope) {
			return true
		}
	}
	return false
}
