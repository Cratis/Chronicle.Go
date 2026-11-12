// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package graphs declares graph projections: projections that fill a property
// graph of nodes and edges instead of a document read model.
//
// A graph projection works like a projection onto a read model. The Chronicle
// kernel observes the events, routes them, and replays them, but its target is a
// graph sink such as Neo4j. Every event becomes a few idempotent operations on
// nodes and edges. The graph is what the events leave behind; nothing writes to it
// directly. The client only declares the graph and registers its definition.
//
// Node and edge types are plain structs. Exactly one field of a node carries
// `chronicle:"key"`, and at most one field of an edge does. That field identifies
// the edge among several edges of the same type between the same two nodes.
//
//	type Person struct {
//		ID   string `json:"id" chronicle:"key"`
//		Name string `json:"name"`
//	}
//
//	person := graphs.NewNode[Person]().
//		From(registered, graphs.UsingKey("personId")).
//		RemovedWith(removed, graphs.UsingKey("personId"), graphs.Detach())
//	group := graphs.NewNode[Group]().
//		From(created, graphs.UsingKey("groupId"))
//	memberOf := graphs.NewEdge[MemberOf](person, group).
//		Named("MEMBER_OF").
//		From(added,
//			graphs.UsingSourceKey("personId"),
//			graphs.UsingTargetKey("groupId"),
//			graphs.SetFromContext("since", "occurred")).
//		RemovedWith(left, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"))
//	graph := graphs.New("Organization", person, group, memberOf)
//
// Register the graph on a chronicle.Registry with Registry.AddGraph. Property
// names in options are serialized names, as in projections: the json tag of the
// field, or the field name under the client's naming policy. They are validated
// when the client is prepared, against the registered event types and the node and
// edge structs.
package graphs
