// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

// SourceFilter narrows a source read. Empty values and Default/All/Default are
// non-narrowing kernel sentinels, not exact routes. Event types match IDs across
// generations. Inputs are borrowed during a read and copied into History.
type SourceFilter struct {
	// SourceType narrows source classification; Default does not narrow.
	SourceType events.SourceType
	// StreamType narrows stream classification; All does not narrow.
	StreamType events.StreamType
	// StreamID narrows stream identity; Default does not narrow.
	StreamID events.StreamID
	// EventTypes selects type IDs; empty means every event type.
	EventTypes []events.TypeRef
}

// FromFilter includes only dimensions supported by the inclusive from-position RPC.
type FromFilter struct {
	// SourceID selects a source when present; nil/empty does not narrow.
	SourceID *events.SourceID
	// EventTypes selects type IDs across generations; empty means all types.
	EventTypes []events.TypeRef
}

// TailFilter selects matching history, using the same fields and sentinel
// semantics as ScopeFilter. Nil/empty dimensions do not narrow reads.
type TailFilter ScopeFilter

// History is one complete matching read, ordered by sequence position. It is
// not a reservation or a server snapshot across multiple calls. Expectation is
// derived ONLY from Events, never from a later tail query.
type History struct {
	// Events owns the ordered loaded events.
	Events []events.Appended
	// Filter is the normalized filter used for this read (including its source).
	Filter ScopeFilter
	// Expectation is NoMatchingEvent for empty history, otherwise Exact(last loaded).
	Expectation Expectation
}

// Scope returns a defensive copy suitable for WithScope or WithScopes. Mutating
// Events cannot advance this expectation to a newer tail.
func (h History) Scope() Scope {
	return cloneScope(Scope{Expectation: h.Expectation, Filter: h.Filter})
}

func sourceReadFilter(source events.SourceID, filter SourceFilter) (ScopeFilter, error) {
	if strings.TrimSpace(string(source)) == "" {
		return ScopeFilter{}, fmt.Errorf("%w: source is required", faults.ErrInvalidConfiguration)
	}
	return normalizeReadFilter(ScopeFilter{SourceID: &source, SourceType: &filter.SourceType, StreamType: &filter.StreamType, StreamID: &filter.StreamID, EventTypes: filter.EventTypes})
}

func normalizeReadFilter(filter ScopeFilter) (ScopeFilter, error) {
	filter.SourceID = readDimension(filter.SourceID, events.SourceID(""))
	filter.SourceType = readDimension(filter.SourceType, events.DefaultSourceType)
	filter.StreamType = readDimension(filter.StreamType, events.AllStreamTypes)
	filter.StreamID = readDimension(filter.StreamID, events.DefaultStreamID)
	refs := append([]events.TypeRef(nil), filter.EventTypes...)
	for i := range refs {
		refs[i].ID = events.TypeID(strings.TrimSpace(string(refs[i].ID)))
		if refs[i].ID == "" || strings.Contains(string(refs[i].ID), ",") || refs[i].Generation == 0 {
			return ScopeFilter{}, fmt.Errorf("%w: invalid read event type", faults.ErrInvalidConfiguration)
		}
		// The query and concurrency validator match IDs, not generations.
		refs[i].Generation = 1
	}
	slices.SortFunc(refs, func(a, b events.TypeRef) int { return strings.Compare(string(a.ID), string(b.ID)) })
	filter.EventTypes = slices.Compact(refs)
	return filter, nil
}

func readDimension[T ~string](value *T, wildcard T) *T {
	if value == nil {
		return nil
	}
	normalized := T(strings.TrimSpace(string(*value)))
	if normalized == "" || normalized == wildcard {
		return nil
	}
	return &normalized
}

func typeIDs(refs []events.TypeRef) string {
	ids := make([]string, len(refs))
	for i, ref := range refs {
		ids[i] = string(ref.ID)
	}
	return strings.Join(ids, ",")
}
