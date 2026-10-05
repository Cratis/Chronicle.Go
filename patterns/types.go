// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package patterns queries server-established behavior. Answers are predictions,
// not promises. The SDK does not mine, rank, train, cache or invent answers.
package patterns

import (
	"maps"
	"time"

	"github.com/cratis/chronicle.go/metadata"
)

// GroupingKey selects whose behavior to query, typically a user. It is a string
// scope, not a UUID or an event-source ID, and grants no authorization. Empty is
// the C# unspecified scope; it is sent to the server unchanged.
type GroupingKey string

// FacetName names a contextual dimension. Unknown names are forwarded unchanged.
type FacetName string

// FacetValue holds a fact. Empty means unspecified; "0" is a specified value.
type FacetValue string

// Well-known facet names retain the C# spelling on the wire. Identity facets are
// query facts, not credentials or authorization claims.
const (
	CommandType       FacetName = "CommandType"
	InitiatorType     FacetName = "InitiatorType"
	InitiatorID       FacetName = "InitiatorId"
	OnBehalfOf        FacetName = "OnBehalfOf"
	CausedByCommand   FacetName = "CausedByCommand"
	CorrelationRootID FacetName = "CorrelationRootId"
	AggregateType     FacetName = "AggregateType"
	Year              FacetName = "Year"
	Month             FacetName = "Month"
	Day               FacetName = "Day"
	TimeBucket        FacetName = "TimeBucket"
)

// FacetSet is an immutable set of contextual facts. Its zero value is empty.
// Like C# FacetSet, empty values are omitted, not constraints on an empty string.
type FacetSet struct{ values map[FacetName]FacetValue }

// NewFacetSet copies facts; subsequent caller changes cannot change the set.
func NewFacetSet(facts map[FacetName]FacetValue) FacetSet {
	values := make(map[FacetName]FacetValue, len(facts))
	for name, value := range facts {
		if value != "" {
			values[name] = value
		}
	}
	return FacetSet{values: values}
}

// With returns a new set with a replaced fact. Empty removes the constraint.
func (f FacetSet) With(name FacetName, value FacetValue) FacetSet {
	values := f.Facts()
	if value == "" {
		delete(values, name)
	} else {
		values[name] = value
	}
	return FacetSet{values: values}
}

// ValueOf returns the value, or the empty unspecified sentinel when absent.
func (f FacetSet) ValueOf(name FacetName) FacetValue { return f.values[name] }

// Facts returns a caller-owned copy. Mutating it cannot alter the set.
func (f FacetSet) Facts() map[FacetName]FacetValue {
	if f.values == nil {
		return map[FacetName]FacetValue{}
	}
	return maps.Clone(f.values)
}

// Specificity returns the number of specified facets.
func (f FacetSet) Specificity() int { return len(f.values) }

// Confidence is the C# double-valued confidence concept. The source does not
// validate its range or finiteness; values are forwarded, including NaN/infinity.
type Confidence float64

// QueryOptions selects optional thresholds. Nil and explicit zero both request
// server defaults, as in C#. Nonpositive limits and confidence are resolved by
// the server, not the SDK. Pointer values are copied before dispatch; callers
// must not mutate them concurrently with the call's initial argument snapshot.
type QueryOptions struct {
	// MinimumConfidence is omitted when nil. Zero is not a zero-confidence override.
	MinimumConfidence *Confidence
	// MaximumResults is omitted when nil. Zero is not a request for zero answers.
	MaximumResults *int32
}

// BehaviorPattern is an owned snapshot in server order. ID and specificity on
// the DTO are descriptive metadata, not UUIDs. Facets are immutable.
type BehaviorPattern struct {
	// ID is the server's canonical facet-set key, not a UUID.
	ID string
	// GroupingKey is the scope the behavior belongs to.
	GroupingKey GroupingKey
	// Facets holds the server-established facts.
	Facets FacetSet
	// Confidence estimates the action's likelihood given its established context.
	Confidence Confidence
	// Support is the share of observed events containing this pattern.
	Support float64
	// Occurrences is the number of observations, not a result-count limit.
	Occurrences int64
	// Weight is recency-weighted strength.
	Weight float64
	// Specificity is the number of facets, derived like the C# client.
	Specificity int
	// FirstSeen preserves the response offset and 100ns DateTimeOffset precision.
	FirstSeen time.Time
	// LastSeen preserves the response offset and 100ns DateTimeOffset precision.
	LastSeen time.Time
}

// Action returns the established action, or unspecified for a context-only pattern.
func (p BehaviorPattern) Action() FacetValue { return p.Facets.ValueOf(CommandType) }

// Scope preserves the server's scope ID and optional display metadata.
type Scope struct {
	// ID is a grouping key, not a UUID.
	ID GroupingKey
	// Name is optional display metadata; it is not an authorization claim.
	Name string
	// UserName is optional display metadata and may contain personal data.
	UserName string
}

// QueryResult carries response correlation and caller-owned data in server order.
// A successful empty response has non-nil empty Data; a failure returns no data.
// The protocol does not provide a total count or resolved threshold/limit.
type QueryResult[T any] struct {
	// CorrelationID is the response correlation; an omitted Guid means Guid.Empty.
	CorrelationID metadata.CorrelationID
	// Data contains independent snapshots; no response or cross-tenant cache exists.
	Data []T
}
