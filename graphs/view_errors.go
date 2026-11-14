// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"errors"
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
)

// ViewReason is the stable reason a graph view was refused. Every reason is an
// error value, so errors.Is(err, graphs.ViewRootNotANode) matches a *ViewError
// carrying that reason. The names are shared by every Chronicle client.
type ViewReason string

// The reasons a view declaration is refused.
const (
	// ViewRootNotANode reports a view whose root is not a node declared in the graph.
	ViewRootNotANode ViewReason = "ViewRootNotANode"
	// ViewIsAlsoSchema reports a view type that is also a node or an edge of the graph.
	ViewIsAlsoSchema ViewReason = "ViewIsAlsoSchema"
	// DuplicateViewName reports two views of a graph sharing a name.
	DuplicateViewName ViewReason = "DuplicateViewName"
	// UnknownNodeProperty reports a property member that names a property the root node does not declare.
	UnknownNodeProperty ViewReason = "UnknownNodeProperty"
	// UnknownEdge reports a relation that follows an edge the graph does not declare.
	UnknownEdge ViewReason = "UnknownEdge"
	// UnknownEdgeProperty reports an edge property member that names a property the edge does not declare.
	UnknownEdgeProperty ViewReason = "UnknownEdgeProperty"
	// EdgeNotIncidentOnRoot reports a relation along an edge that does not touch the root label of the view.
	EdgeNotIncidentOnRoot ViewReason = "EdgeNotIncidentOnRoot"
	// AmbiguousDirection reports a relation along a self edge that gives no explicit direction.
	AmbiguousDirection ViewReason = "AmbiguousDirection"
	// TargetLabelMismatch reports a relation whose target view is not rooted at the far end of the edge.
	TargetLabelMismatch ViewReason = "TargetLabelMismatch"
	// RelationTargetNotAView reports a relation whose target is not a view declared in the graph.
	RelationTargetNotAView ViewReason = "RelationTargetNotAView"
	// RecursiveView reports a view that contains itself, directly or transitively.
	RecursiveView ViewReason = "RecursiveView"
	// EdgePropertyViewNotUsedAsTarget reports a view with edge property members that no relation targets.
	EdgePropertyViewNotUsedAsTarget ViewReason = "EdgePropertyViewNotUsedAsTarget"
	// EdgePropertyViewSharedAcrossEdges reports a view with edge property members that relations of
	// more than one edge type target.
	EdgePropertyViewSharedAcrossEdges ViewReason = "EdgePropertyViewSharedAcrossEdges"
)

// Error returns the reason, which makes every ViewReason usable as a sentinel.
func (r ViewReason) Error() string { return string(r) }

// ViewError locates a refused view declaration. It matches its Reason and
// chronicle.ErrInvalidConfiguration with errors.Is.
type ViewError struct {
	// Graph is the name of the graph.
	Graph string
	// View is the name of the view, empty when the refusal is not about one view.
	View string
	// Member is the name of the view member, empty when the refusal is about the view as a whole.
	Member string
	// Reason is the stable reason code.
	Reason ViewReason
	// Detail explains the refusal.
	Detail string
}

func (e *ViewError) Error() string {
	where := "graph " + e.Graph
	if e.View != "" {
		where += " view " + e.View
	}
	if e.Member != "" {
		where += " member " + e.Member
	}
	if e.Detail == "" {
		return fmt.Sprintf("chronicle: %s: %s", where, e.Reason)
	}
	return fmt.Sprintf("chronicle: %s: %s: %s", where, e.Reason, e.Detail)
}

// Unwrap exposes the reason and chronicle.ErrInvalidConfiguration.
func (e *ViewError) Unwrap() []error { return []error{e.Reason, faults.ErrInvalidConfiguration} }

// ViewReasonOf returns the reason of a refused view declaration, if err is or
// wraps a *ViewError.
func ViewReasonOf(err error) (ViewReason, bool) {
	var viewErr *ViewError
	if errors.As(err, &viewErr) {
		return viewErr.Reason, true
	}
	return "", false
}

func refuseView(graph, view, member string, reason ViewReason, format string, args ...any) error {
	return &ViewError{Graph: graph, View: view, Member: member, Reason: reason, Detail: fmt.Sprintf(format, args...)}
}
