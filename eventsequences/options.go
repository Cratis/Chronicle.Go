// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
)

// Route selects an append route. Empty fields resolve to Default/All/Default.
type Route struct {
	// SourceType classifies the event source.
	SourceType events.SourceType
	// StreamType classifies the stream.
	StreamType events.StreamType
	// StreamID identifies the stream independently of source ID.
	StreamID events.StreamID
}

// Expectation is a tagged concurrency expectation. The zero value means Resolve,
// not Exact(0). Reserved numeric positions are rejected before dispatch.
type Expectation struct {
	kind     uint8
	position events.SequenceNumber
}

// Resolve asks the optimistic strategy to query the matching tail at append time.
func Resolve() Expectation { return Expectation{} }

// Exact supplies an explicit upper bound: the kernel rejects a matching tail
// greater than position, but accepts a lower tail or no matching history. It does
// not require equality. Use NoMatchingEvent to require an empty matching history.
func Exact(position events.SequenceNumber) Expectation {
	return Expectation{kind: 1, position: position}
}

// NoMatchingEvent protects the absence of any matching history.
func NoMatchingEvent() Expectation { return Expectation{kind: 2} }

// NoCheck explicitly disables concurrency checking.
func NoCheck() Expectation { return Expectation{kind: 3} }

// ScopeFilter describes the exact matching history. Nil dimensions do not narrow.
// A single Append can only bind SourceID to its own target; other sources are rejected.
type ScopeFilter struct {
	// SourceID narrows to this source when present.
	SourceID *events.SourceID
	// SourceType narrows by source classification (Default is a kernel wildcard).
	SourceType *events.SourceType
	// StreamType narrows by stream classification (All is a kernel wildcard).
	StreamType *events.StreamType
	// StreamID narrows by stream identity (Default is a kernel wildcard).
	StreamID *events.StreamID
	// EventTypes narrows by type identity; generation is validated but the kernel tail query filters IDs.
	EventTypes []events.TypeRef
}

// Scope combines a concurrency expectation and history filter.
type Scope struct {
	// Expectation defaults to Resolve.
	Expectation Expectation
	// Filter selects matching history.
	Filter ScopeFilter
}

// AppendOption configures one append. Scalar options are last-wins. Nil options
// fail. Slice and pointer inputs are copied when the option is constructed.
type AppendOption func(*appendConfig)
type appendConfig struct {
	route       Route
	scope       *Scope
	occurred    *time.Time
	subject     *events.Subject
	correlation metadata.CorrelationID
	tags        []events.Tag
	named       []events.NamedTag
}

// WithRoute overrides routing; it never changes the source argument.
func WithRoute(route Route) AppendOption { return func(c *appendConfig) { c.route = route } }

// WithScope overrides the default source-and-route optimistic scope.
func WithScope(scope Scope) AppendOption {
	f := &scope.Filter
	f.SourceID, f.SourceType, f.StreamType, f.StreamID = copyPointer(f.SourceID), copyPointer(f.SourceType), copyPointer(f.StreamType), copyPointer(f.StreamID)
	f.EventTypes = append([]events.TypeRef(nil), f.EventTypes...)
	return func(c *appendConfig) { copy := scope; c.scope = &copy }
}

// WithOccurred supplies occurrence time, truncated to .NET-compatible 100ns precision.
func WithOccurred(occurred time.Time) AppendOption {
	return func(c *appendConfig) { c.occurred = &occurred }
}

// WithSubject overrides the source-derived compliance subject; empty subjects are invalid.
func WithSubject(subject events.Subject) AppendOption {
	return func(c *appendConfig) { c.subject = &subject }
}

// WithCorrelation overrides the context correlation. A zero ID requests a new UUID.
func WithCorrelation(id metadata.CorrelationID) AppendOption {
	return func(c *appendConfig) { c.correlation = id }
}

// WithTags sets dynamic tags merged distinctly after static tags.
func WithTags(tags ...events.Tag) AppendOption {
	copy := append([]events.Tag(nil), tags...)
	return func(c *appendConfig) { c.tags = copy }
}

// WithNamedTags supplies exact name/value records. Duplicate records are coalesced;
// different values under one name remain distinct.
func WithNamedTags(tags ...events.NamedTag) AppendOption {
	copy := append([]events.NamedTag(nil), tags...)
	return func(c *appendConfig) { c.named = copy }
}

func copyPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
