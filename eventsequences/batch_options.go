// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
)

// Entry describes one event in an ordered atomic batch. Inputs are borrowed only
// during AppendBatch; callers must not mutate them concurrently with the call.
type Entry struct {
	// Source is the nonblank event source to append to.
	Source events.SourceID
	// Event is a registered value or non-nil pointer.
	Event any
	// Route selects the append route; zero fields use Default/All/Default.
	Route Route
	// Tags merges after the event type's static tags and before batch tags.
	Tags []events.Tag
	// NamedTags preserves exact name/value pairs, including empty values.
	NamedTags []events.NamedTag
	// Occurred overrides server-assigned occurrence time when non-nil.
	Occurred *time.Time
	// Subject overrides the default compliance subject (Source).
	Subject *events.Subject
	// Causation extends the context's chain for this entry only, in input order.
	Causation []metadata.Causation
}

// LabeledScope enrolls a concurrency check in the same atomic batch. Labels must
// be nonblank and unique. If Scope narrows SourceID, it must equal Label. Labels
// need not have an appended event, so independent histories can be protected.
type LabeledScope struct {
	// Label identifies the check in returned concurrency violations.
	Label string
	// Scope specifies the expectation and exact narrowing to check.
	Scope Scope
}

// BatchOption configures an atomic batch. Scalar options are last-wins; nil
// options fail. Slice/map/pointer inputs are snapshotted at option construction.
type BatchOption func(*batchConfig)
type batchConfig struct {
	correlation metadata.CorrelationID
	tags        []events.Tag
	named       []events.NamedTag
	scopes      []LabeledScope
}

// WithScopes sets explicit checks. A label matching an appended source replaces
// its default optimistic check; other labels add independent checks. Resolve
// queries precisely its Filter. Omitted sources use their first entry's route.
func WithScopes(scopes ...LabeledScope) BatchOption {
	copy := append([]LabeledScope(nil), scopes...)
	for i := range copy {
		copy[i].Scope = cloneScope(copy[i].Scope)
	}
	return func(c *batchConfig) { c.scopes = copy }
}

// WithBatchCorrelation overrides context correlation; zero requests a new UUID.
func WithBatchCorrelation(id metadata.CorrelationID) BatchOption {
	return func(c *batchConfig) { c.correlation = id }
}

// WithBatchTags sets ordinary tags merged into every entry.
func WithBatchTags(tags ...events.Tag) BatchOption {
	copy := append([]events.Tag(nil), tags...)
	return func(c *batchConfig) { c.tags = copy }
}

// WithBatchNamedTags sets named tags merged distinctly after every entry's tags.
func WithBatchNamedTags(tags ...events.NamedTag) BatchOption {
	copy := append([]events.NamedTag(nil), tags...)
	return func(c *batchConfig) { c.named = copy }
}

func cloneScope(scope Scope) Scope {
	f := &scope.Filter
	f.SourceID, f.SourceType, f.StreamType, f.StreamID = copyPointer(f.SourceID), copyPointer(f.SourceType), copyPointer(f.StreamType), copyPointer(f.StreamID)
	f.EventTypes = append([]events.TypeRef(nil), f.EventTypes...)
	return scope
}

func normalizedRoute(route Route) Route {
	if route.SourceType == "" {
		route.SourceType = events.DefaultSourceType
	}
	if route.StreamType == "" {
		route.StreamType = events.AllStreamTypes
	}
	if route.StreamID == "" {
		route.StreamID = events.DefaultStreamID
	}
	return route
}

func defaultScope(source events.SourceID, route Route) Scope {
	route = normalizedRoute(route)
	return Scope{Filter: ScopeFilter{SourceID: &source, SourceType: &route.SourceType, StreamType: &route.StreamType, StreamID: &route.StreamID}}
}
