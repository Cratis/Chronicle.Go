// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"reflect"

	"github.com/cratis/chronicle.go/events"
)

// Event is a registered event type, as returned by chronicle.RegisterEvent. The
// graph only needs its identity; the registry resolves the rest when the client
// is prepared.
type Event interface {
	Descriptor() events.Descriptor
}

// structType is T, or the struct a pointer type T points to.
func structType[T any]() reflect.Type {
	typ := reflect.TypeFor[T]()
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}

// Element is a node or an edge, accepted by New.
type Element interface{ element() }

// Node declares a node type of a graph and the events that create, change and
// remove it. It is not safe for concurrent mutation. Declaration errors are
// collected and reported by Graph.Build, so a declaration reads as one chain.
type Node struct {
	typ         reflect.Type
	label       string
	key         string
	from        []*nodeFrom
	removedWith []*nodeRemoval
	problems    []error
}

func (*Node) element() {}

// NewNode declares T, a struct or a pointer to one, as a node type. The label of the node is the name
// of T unless Labeled says otherwise. The key is the one field of T that carries
// `chronicle:"key"`, unless IdentifiedBy names another one.
func NewNode[T any]() *Node {
	typ := structType[T]()
	return &Node{typ: typ, label: typ.Name()}
}

// Labeled names the node label, which is how the node is known in the graph. It
// defaults to the name of the node type.
func (n *Node) Labeled(label string) *Node {
	n.label = label
	return n
}

// IdentifiedBy names the serialized property that holds the unique key of the
// node. It is not needed when the property carries `chronicle:"key"`.
func (n *Node) IdentifiedBy(property string) *Node {
	n.key = property
	return n
}

// From creates or changes the node when event is observed. By default the key is
// the event source id, and every property of the node is mapped from the event
// property of the same name, if the event has one.
func (n *Node) From(event Event, options ...NodeFromOption) *Node {
	from := &nodeFrom{event: event}
	for _, option := range options {
		if option == nil {
			n.problems = append(n.problems, errNilOption)
			continue
		}
		option.applyNodeFrom(from)
	}
	n.from = append(n.from, from)
	return n
}

// RemovedWith deletes the node when event is observed. By default the key is the
// event source id. A node that still has edges cannot be deleted unless Detach
// deletes them with it.
func (n *Node) RemovedWith(event Event, options ...NodeRemovalOption) *Node {
	removal := &nodeRemoval{event: event}
	for _, option := range options {
		if option == nil {
			n.problems = append(n.problems, errNilOption)
			continue
		}
		option.applyNodeRemoval(removal)
	}
	n.removedWith = append(n.removedWith, removal)
	return n
}

// Edge declares an edge type that connects two nodes of a graph and the events
// that create, change and remove it. It is not safe for concurrent mutation.
type Edge struct {
	typ         reflect.Type
	name        string
	source      *Node
	target      *Node
	identity    string
	from        []*edgeFrom
	removedWith []*edgeRemoval
	problems    []error
}

func (*Edge) element() {}

// NewEdge declares T, a struct, as an edge type from source to target. The nodes
// must be part of the same graph. The edge type is the name of T unless Named says
// otherwise. T holds the properties of the edge; the end nodes are not properties.
// T can be an empty struct for an edge without properties.
func NewEdge[T any](source, target *Node) *Edge {
	typ := structType[T]()
	return &Edge{typ: typ, name: typ.Name(), source: source, target: target}
}

// Named names the edge type, for example "MEMBER_OF". It defaults to the name of
// the edge type.
func (e *Edge) Named(name string) *Edge {
	e.name = name
	return e
}

// IdentifiedBy names the serialized property that identifies the edge among edges
// of the same type between the same two nodes. It is not needed when the property
// carries `chronicle:"key"`. Without an identifying property there is at most one
// edge of a type between two nodes.
func (e *Edge) IdentifiedBy(property string) *Edge {
	e.identity = property
	return e
}

// From creates or changes the edge when event is observed. Both end nodes are
// created when they do not exist. By default both keys are the event source id.
func (e *Edge) From(event Event, options ...EdgeFromOption) *Edge {
	from := &edgeFrom{event: event}
	for _, option := range options {
		if option == nil {
			e.problems = append(e.problems, errNilOption)
			continue
		}
		option.applyEdgeFrom(from)
	}
	e.from = append(e.from, from)
	return e
}

// RemovedWith deletes the edge when event is observed. The nodes at both ends stay.
func (e *Edge) RemovedWith(event Event, options ...EdgeRemovalOption) *Edge {
	removal := &edgeRemoval{event: event}
	for _, option := range options {
		if option == nil {
			e.problems = append(e.problems, errNilOption)
			continue
		}
		option.applyEdgeRemoval(removal)
	}
	e.removedWith = append(e.removedWith, removal)
	return e
}
