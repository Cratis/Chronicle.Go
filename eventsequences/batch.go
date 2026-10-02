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
	batch, err := s.prepareBatch(ctx, entries, batchConfig{correlation: config.correlation, tags: tags, named: config.named, scopes: []LabeledScope{{Label: string(source), Scope: scope}}})
	if err != nil {
		return BatchResult{}, err
	}
	if config.route == normalizedRoute(Route{}) {
		return s.dispatchMany(ctx, source, config, batch)
	}
	return s.dispatchBatch(ctx, batch)
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
	batch, err := s.prepareBatch(ctx, entries, config)
	if err != nil {
		return BatchResult{}, err
	}
	return s.dispatchBatch(ctx, batch)
}

type preparedBatch struct {
	request *sequences.AppendManyForEventSourcesRequest
	named   [][]*sequences.NamedTag
	refs    []events.TypeRef
}

func (s *Sequence) prepareBatch(ctx context.Context, entries []Entry, config batchConfig) (preparedBatch, error) {
	if err := validateAppendMetadata(nil, nil, config.named); err != nil {
		return preparedBatch{}, err
	}
	if config.correlation == (metadata.CorrelationID{}) {
		var err error
		config.correlation, err = metadata.NewCorrelationID()
		if err != nil {
			return preparedBatch{}, err
		}
	}
	request := &sequences.AppendManyForEventSourcesRequest{EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id), CorrelationId: wire.Guid(config.correlation), Causation: causationContract(metadata.CausationChain(ctx)), CausedBy: identityContract(metadata.Identity(ctx))}
	batch := preparedBatch{request: request}
	for _, entry := range entries {
		if strings.TrimSpace(string(entry.Source)) == "" {
			return preparedBatch{}, faults.ErrInvalidConfiguration
		}
		descriptor, found := s.catalog.Lookup(entry.Event)
		if !found {
			return preparedBatch{}, faults.ErrNotRegistered
		}
		content, err := descriptor.Marshal(entry.Event)
		if err != nil {
			return preparedBatch{}, err
		}
		options := appendConfig{route: entry.Route, scope: &Scope{Expectation: NoCheck()}, correlation: config.correlation, subject: entry.Subject, occurred: entry.Occurred,
			tags: append(append([]events.Tag(nil), entry.Tags...), config.tags...), named: append(append([]events.NamedTag(nil), entry.NamedTags...), config.named...)}
		one, err := s.request(ctx, entry.Source, descriptor, string(content), options)
		if err != nil {
			return preparedBatch{}, err
		}
		event := &sequences.EventForEventSourceId{EventSourceId: one.EventSourceId, EventSourceType: one.EventSourceType, EventStreamType: one.EventStreamType, EventStreamId: one.EventStreamId, EventType: one.EventType, Content: one.Content, Tags: one.Tags, Subject: one.Subject, Occurred: one.Occurred}
		if len(entry.Causation) > 0 {
			chain := metadata.CausationChain(ctx)
			for _, cause := range entry.Causation {
				if cause.Occurred.Year() < 1 || cause.Occurred.Year() > 9999 {
					return preparedBatch{}, faults.ErrInvalidConfiguration
				}
				chain = append(chain, cause)
			}
			event.Causation = causationContract(chain)
		}
		request.Events = append(request.Events, event)
		batch.named = append(batch.named, namedRequest(one, options.named).NamedTags)
		batch.refs = append(batch.refs, descriptor.Ref())
	}
	scopes, err := batchScopes(entries, config.scopes)
	if err != nil {
		return preparedBatch{}, err
	}
	for _, labeled := range scopes {
		scope, err := s.resolveLabeledScope(ctx, events.SourceID(labeled.Label), labeled.Scope)
		if err != nil {
			return preparedBatch{}, err
		}
		request.ConcurrencyScopes = append(request.ConcurrencyScopes, &sequences.EventSourceConcurrencyScope{EventSourceId: labeled.Label, Scope: scope})
	}
	if len(entries) == 0 && !hasProtectedScope(request.ConcurrencyScopes) {
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
