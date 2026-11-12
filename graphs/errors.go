// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"errors"
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Sentinel errors identify why a graph declaration was refused. Every refusal is
// a *DeclarationError, so errors.Is matches both the category below and
// chronicle.ErrInvalidConfiguration.
var (
	// ErrNodeKey reports a node without exactly one key property.
	ErrNodeKey = errors.New("a node must have exactly one key property")
	// ErrEdgeEndpoints reports an edge without a source node and a target node
	// that are both part of the same graph.
	ErrEdgeEndpoints = errors.New("an edge must have one source node and one target node of the graph")
	// ErrEdgeIdentity reports an edge with more than one identifying key, or an
	// event that cannot supply the identity of an edge that has one.
	ErrEdgeIdentity = errors.New("an edge can have only one identifying key, supplied by every event")
	// ErrNestedObject reports a node or edge property that is an object. A graph
	// property is a primitive or a list of primitives.
	ErrNestedObject = errors.New("a node or edge property cannot be an object")
	// ErrUnknownProperty reports a mapping that names a property the node, the edge
	// or the event does not have, or one that cannot take part in a mapping.
	ErrUnknownProperty = errors.New("unknown property")
	// ErrInvalidMapping reports a mapping that cannot be expressed by the kernel,
	// for example a literal that is not representable or a duplicate mapping.
	ErrInvalidMapping = errors.New("invalid property mapping")
	// ErrDuplicateElement reports two nodes or edges sharing a label or type.
	ErrDuplicateElement = errors.New("duplicate node label or edge type")
	// ErrElementIsReadModel reports a node or edge struct that is also registered
	// as a read model.
	ErrElementIsReadModel = errors.New("a node or edge cannot also be a read model")
	// ErrInvalidGraph reports any other invalid declaration, such as a missing name.
	ErrInvalidGraph = errors.New("invalid graph")
)

// DeclarationError locates a refused declaration. Err is one of the sentinels in
// this package, or a wrapped registry error such as chronicle.ErrNotRegistered.
type DeclarationError struct {
	// Graph is the name of the graph.
	Graph string
	// Element is the label of the node or the type of the edge, empty for the graph itself.
	Element string
	// Detail explains the refusal. It never contains mapped values.
	Detail string
	// Err is the category of the refusal.
	Err error
}

func (e *DeclarationError) Error() string {
	where := "graph " + e.Graph
	if e.Element != "" {
		where += " element " + e.Element
	}
	if e.Detail == "" {
		return fmt.Sprintf("chronicle: %s: %v", where, e.Err)
	}
	return fmt.Sprintf("chronicle: %s: %v: %s", where, e.Err, e.Detail)
}

// Unwrap exposes the category and chronicle.ErrInvalidConfiguration.
func (e *DeclarationError) Unwrap() []error { return []error{e.Err, faults.ErrInvalidConfiguration} }

func refuse(graph, element string, category error, format string, args ...any) error {
	return &DeclarationError{Graph: graph, Element: element, Detail: fmt.Sprintf(format, args...), Err: category}
}
