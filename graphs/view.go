// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import "reflect"

// View declares a graph view: a read model over an existing property graph. A
// view is anchored at the root node and its members are properties of that
// node, relations along declared edges to other views, or properties of the edge
// that was followed. A view only reads. It is not safe for concurrent mutation.
// Declaration errors are collected and reported by Graph.Build, so a view reads
// as one chain.
//
// T is the Go type the view is read into. It names the view and must not also be
// a node or an edge of the graph. Member names are serialized property names of
// T, the way property names of nodes and edges are.
type View struct {
	typ      reflect.Type
	name     string
	root     *Node
	members  []viewMember
	problems []error
}

type viewMember struct {
	name        string
	kind        ViewMemberKind
	source      string
	edge        *Edge
	target      *View
	direction   ViewDirection
	cardinality ViewCardinality
}

// NewView declares T, a struct or a pointer to one, as a view over the graph
// rooted at root. The name of the view is the name of T unless Named says
// otherwise. The root must be a node of the graph the view is added to.
func NewView[T any](root *Node) *View {
	typ := structType[T]()
	return &View{typ: typ, name: typ.Name(), root: root}
}

// Named names the view. It defaults to the name of the view type. View names are
// unique in a graph.
func (v *View) Named(name string) *View {
	v.name = name
	return v
}

// PropertyOption configures a property member of a view.
type PropertyOption interface{ applyProperty(*viewMember) }

type sourceProperty string

func (o sourceProperty) applyProperty(m *viewMember) { m.source = string(o) }

// SourceProperty names the property of the root node, or of the followed edge for
// an edge property, that the member reads. Without it the member is matched to
// the property of the same name, ignoring case.
func SourceProperty(name string) PropertyOption { return sourceProperty(name) }

// FollowOption configures a relation member of a view.
type FollowOption interface{ applyFollow(*viewMember) }

type direction ViewDirection

func (o direction) applyFollow(m *viewMember) { m.direction = ViewDirection(o) }

// Outgoing follows the edge from its source node to its target node. The
// direction of an edge that is not a self edge is derived from the root of the
// view; a self edge needs an explicit direction.
func Outgoing() FollowOption { return direction(ViewOutgoing) }

// Incoming follows the edge from its target node to its source node. See Outgoing.
func Incoming() FollowOption { return direction(ViewIncoming) }

// Property adds a property member that reads a property of the root node. The
// node key is an ordinary property.
func (v *View) Property(member string, options ...PropertyOption) *View {
	return v.addProperty(ViewProperty, member, options)
}

// EdgeProperty adds a member that reads a property of the edge that was
// followed to reach the view. It is only valid in a view that is the target of
// relations along exactly one edge type.
func (v *View) EdgeProperty(member string, options ...PropertyOption) *View {
	return v.addProperty(ViewEdgeProperty, member, options)
}

func (v *View) addProperty(kind ViewMemberKind, member string, options []PropertyOption) *View {
	entry := viewMember{name: member, kind: kind}
	for _, option := range options {
		if option == nil {
			v.problems = append(v.problems, errNilOption)
			continue
		}
		option.applyProperty(&entry)
	}
	if entry.source == "" {
		entry.source = member
	}
	v.members = append(v.members, entry)
	return v
}

// FollowsMany adds a relation member that follows edge to target and yields a
// list. A node without related nodes yields an empty list. The target view must
// be added to the same graph and be rooted at the far end of the edge.
func (v *View) FollowsMany(member string, edge *Edge, target *View, options ...FollowOption) *View {
	return v.follow(ViewMany, member, edge, target, options)
}

// FollowsOne adds a relation member that follows edge to target and yields a
// single value. A node without a related node yields null.
func (v *View) FollowsOne(member string, edge *Edge, target *View, options ...FollowOption) *View {
	return v.follow(ViewOne, member, edge, target, options)
}

func (v *View) follow(cardinality ViewCardinality, member string, edge *Edge, target *View, options []FollowOption) *View {
	entry := viewMember{name: member, kind: ViewRelation, edge: edge, target: target, cardinality: cardinality}
	for _, option := range options {
		if option == nil {
			v.problems = append(v.problems, errNilOption)
			continue
		}
		option.applyFollow(&entry)
	}
	v.members = append(v.members, entry)
	return v
}

// WithViews adds views to the graph. Every view a relation targets must be added,
// and every view is read over the nodes and edges of this graph. Views do not
// change the graph projection; they only describe how the graph is read.
func (g *Graph) WithViews(views ...*View) *Graph {
	g.views = append(g.views, views...)
	return g
}

// viewSnapshot is the frozen form of a view. Nodes, edges and target views are
// resolved to the frozen declaration; one that is not part of the graph stays nil
// and is refused by validation.
type viewSnapshot struct {
	typ      reflect.Type
	name     string
	root     *Node
	members  []memberSnapshot
	problems []error
}

type memberSnapshot struct {
	viewMember
	edge   *Edge
	target *viewSnapshot
}

// freezeViews resolves views against the frozen nodes and edges of a graph.
func freezeViews(views []*View, nodes map[*Node]*Node, edges map[*Edge]*Edge) []*viewSnapshot {
	index := make(map[*View]*viewSnapshot, len(views))
	var result []*viewSnapshot
	for _, view := range views {
		if view == nil || index[view] != nil {
			continue
		}
		index[view] = &viewSnapshot{typ: view.typ, name: view.name, root: nodes[view.root]}
		result = append(result, index[view])
	}
	for view, snapshot := range index {
		snapshot.problems = append(snapshot.problems, view.problems...)
		for _, member := range view.members {
			snapshot.members = append(snapshot.members, memberSnapshot{
				viewMember: member,
				edge:       edges[member.edge],
				target:     index[member.target],
			})
		}
	}
	return result
}
