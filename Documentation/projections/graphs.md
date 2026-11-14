---
title: Graph projections
description: Declare graph projections in Go: nodes and edges filled from events, registered with the Chronicle kernel and stored in Neo4j or an in-memory graph.
---

Use a graph projection when the answer to your question is a relationship: who
belongs to which group, which product is part of which bundle, who reports to
whom. A graph projection works like a [projection onto a read model](index.md):
the same observer, event routing, replay and failed-partition handling. Its target
is a **property graph** of nodes and edges, stored by a graph sink such as Neo4j.
Every event becomes a few idempotent operations on nodes and edges. The graph is
what the events leave behind; nothing writes to it directly.

Your Go process declares the graph and registers its definition. The Chronicle
kernel runs it. The Go client does not read the graph back: query it with Cypher
or the tooling of the graph database, or declare [views](#read-a-graph-with-views)
over it. Graph projections need Chronicle 19.129.0 or later, and the client produces the same definition as the .NET client.

## Declare a graph

These declarations come from the [runnable example](https://github.com/Cratis/Chronicle.Go/blob/main/examples/graphs/main.go).
Start with the events. They say what happened; they know nothing about the graph.

```go
// The events say what happened. Nodes and edges are what they leave behind.
type PersonRegistered struct {
    PersonID string `json:"personId"`
    Name string `json:"name"`
}

type PersonRemoved struct {
    PersonID string `json:"personId"`
}

type GroupCreated struct {
    GroupID string `json:"groupId"`
    Title string `json:"title"`
}

type MemberJoined struct {
    PersonID string `json:"personId"`
    GroupID string `json:"groupId"`
}

type MemberLeft struct {
    PersonID string `json:"personId"`
    GroupID string `json:"groupId"`
}
```

Nodes and edges are plain structs. Declare them with `graphs.NewNode` and
`graphs.NewEdge`, say which events create, change and remove them, and add them to
a graph. `Build` validates the declaration and `AddGraph` admits it to the registry.

```go
// Person is a node. The field tagged key identifies it.
type Person struct {
    ID string `json:"id" chronicle:"key"`
    Name string `json:"name"`
}

// Group is a node with a counter that follows the members.
type Group struct {
    ID string `json:"id" chronicle:"key"`
    Title string `json:"title"`
    MemberCount int `json:"memberCount"`
}

// MemberOf is an edge. Its end nodes are not properties; the graph names them.
type MemberOf struct {
    Since time.Time `json:"since"`
}

func declarations() (*chronicle.Registry, error) {
    registry := chronicle.NewRegistry()
    registered, err := chronicle.RegisterEvent[PersonRegistered](registry, events.WithID("person-registered"))
    if err != nil {
        return nil, err
    }
    removed, err := chronicle.RegisterEvent[PersonRemoved](registry, events.WithID("person-removed"))
    if err != nil {
        return nil, err
    }
    created, err := chronicle.RegisterEvent[GroupCreated](registry, events.WithID("group-created"))
    if err != nil {
        return nil, err
    }
    joined, err := chronicle.RegisterEvent[MemberJoined](registry, events.WithID("member-joined"))
    if err != nil {
        return nil, err
    }
    left, err := chronicle.RegisterEvent[MemberLeft](registry, events.WithID("member-left"))
    if err != nil {
        return nil, err
    }
    person := graphs.NewNode[Person]().
        From(registered, graphs.UsingKey("personId")).
        RemovedWith(removed, graphs.UsingKey("personId"), graphs.Detach())
    group := graphs.NewNode[Group]().
        From(created, graphs.UsingKey("groupId")).
        From(joined, graphs.UsingKey("groupId"), graphs.Increment("memberCount")).
        From(left, graphs.UsingKey("groupId"), graphs.Decrement("memberCount"))
    memberOf := graphs.NewEdge[MemberOf](person, group).
        Named("MEMBER_OF").
        From(joined,
            graphs.UsingSourceKey("personId"),
            graphs.UsingTargetKey("groupId"),
            graphs.SetFromContext("since", "occurred")).
        RemovedWith(left, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"))
    declaration, err := graphs.New("Organization", person, group, memberOf).Build()
    if err != nil {
        return nil, err
    }
    return registry, registry.AddGraph(declaration)
}
```

The name of the graph, `Organization`, is also the identifier of its projection and
of the read model the graph is registered as. It must not collide with another
read model, projection, reactor or reducer. A graph is registered in the same pass
as every other projection of the store, after event types and before observers
start. Registering after `NewClient` does not change that client.

## Nodes

A node type has exactly one field tagged `chronicle:"key"`: its unique key. Use
`IdentifiedBy("property")` instead when you cannot tag the field. The node label,
how the node is known in the graph, is the name of the struct; `Labeled` changes it.

| Declaration | Meaning |
| --- | --- |
| `From(event, options...)` | Create or change the node when the event is observed |
| `RemovedWith(event, options...)` | Delete the node when the event is observed |
| `UsingKey("eventProperty")` | The event property holding the node key; without it the key is the **event source id** |
| `OnlyWhenExisting()` | Change the node only when it exists; the event never creates it |
| `Detach()` | On `RemovedWith`: delete the edges of the node with it. A node that still has edges cannot be deleted without it |

## Edges

`NewEdge[T](source, target)` connects two nodes of the same graph. The edge type is
the name of `T`; `Named("MEMBER_OF")` changes it. `T` holds the properties of the
edge and can be an empty struct for an edge without properties. By default an edge
is identified by its type and its end nodes, so there is at most one edge of a type
between two nodes. Tag one field `chronicle:"key"`, or use `IdentifiedBy`, to allow
several. Every event of such an edge must then supply the identity, from the event
property of the same name or from `UsingIdentity("eventProperty")`.

| Declaration | Meaning |
| --- | --- |
| `From(event, options...)` | Create or change the edge; both end nodes are created when they do not exist |
| `RemovedWith(event, options...)` | Delete the edge. The nodes at both ends stay. An edge cannot detach |
| `UsingSourceKey`, `UsingTargetKey` | Event properties holding the keys of the end nodes; without them, the event source id |
| `UsingIdentity` | The event property holding the identity of an identified edge |

## Map properties

Properties of a node or edge that the event has under the same name are mapped
without being asked to, except the key. The options below map the rest. A property
name is a serialized name: the `json` tag of the field, or the field name under the
client's [naming policy](../serialization.md). Both sides are validated when the
client is prepared, against the registered event types and the structs.

| Option | Expression sent to the kernel |
| --- | --- |
| `Set("property", "eventProperty")` | The event property |
| `SetFromContext("property", "occurred")` | `$eventContext(occurred)`; also `eventSourceId`, `sequenceNumber`, `correlationId` and the other scalar context properties |
| `SetFromEventSourceID("property")` | `$eventSourceId` |
| `SetValue("property", value)` | `$value(...)`, or `$null` for `nil`; strings, booleans, numbers and `time.Time` |
| `Add`, `Subtract` | A running total of an event property: `$add(...)`, `$subtract(...)` |
| `Increment`, `Decrement`, `Count` | `$increment`, `$decrement`, `$count` on a numeric property |
| `Clear("property")` | `$null` |

A mapping that cannot work is refused before any connection: unknown properties,
a string stored in a number, a counter on a string. The kernel's literal grammar
cannot represent every string; commas, semicolons, parentheses, quotes and line
breaks in `SetValue` are refused instead of being misencoded.

## Where the graph is stored

A graph is stored in Neo4j unless you say otherwise. `UsingSink` selects the sink
of one graph and `chronicle.WithDefaultGraphSinkType` the default of the client;
both accept `graphs.Neo4j` and `graphs.InMemoryGraph`. The in-memory graph needs no
database and suits development and specifications. Selecting a sink does not
configure the Neo4j connection: that belongs to the kernel.

## Read a graph with views

A **graph view** is a read model over a graph you have already declared. It is
anchored at one node, and each member is a property of that node, a relation along
a declared edge to another view, or a property of the edge that was followed. A view
only reads: it changes nothing about the projection, the read model schema or what
the kernel stores. Graphs that declare no views behave exactly as before.

Declare views with `graphs.NewView` and add them to the graph with `WithViews`. The
view type names the view (`Named` overrides it); member names are serialized
property names of that type.

```go
// GroupMembership is read from a group and the MEMBER_OF edge that led to it.
type GroupMembership struct {
    ID    string    `json:"id"`
    Title string    `json:"title"`
    Since time.Time `json:"since"`
}

// PersonWithGroups is read from a person and the groups the person is a member of.
type PersonWithGroups struct {
    ID     string            `json:"id"`
    Name   string            `json:"name"`
    Groups []GroupMembership `json:"groups"`
}

func organizationWithViews(person, group *graphs.Node, memberOf *graphs.Edge) (graphs.Declaration, error) {
    groupMembership := graphs.NewView[GroupMembership](group).
        Property("id").
        Property("title").
        EdgeProperty("since")
    personWithGroups := graphs.NewView[PersonWithGroups](person).
        Property("id").
        Property("name").
        FollowsMany("groups", memberOf, groupMembership)
    return graphs.New("Organization", person, group, memberOf).
        WithViews(personWithGroups, groupMembership).
        Build()
}
```

| Declaration | Meaning |
| --- | --- |
| `NewView[T](root)` | A view named after `T`, anchored at the node `root`, which must be a node of the graph |
| `Property(member, options...)` | Read a property of the root node, matched by name ignoring case; `SourceProperty` names a different one |
| `EdgeProperty(member, options...)` | Read a property of the edge that was followed to reach this view |
| `FollowsMany(member, edge, view, options...)` | Follow `edge` to `view` and yield a list; no related nodes gives an empty list |
| `FollowsOne(member, edge, view, options...)` | Follow `edge` to `view` and yield one value; no related node gives null |
| `Outgoing()`, `Incoming()` | The direction of a relation. It is derived from the root for every edge except a self edge, which needs one of them |

`Build` refuses an invalid view with a `*graphs.ViewError`. Match its stable reason
with `errors.Is` against a `graphs.ViewReason`, or read it with
`graphs.ViewReasonOf`. The error also matches `chronicle.ErrInvalidConfiguration`.
A refused view fails the `Build` of its own graph only; other graphs are
unaffected.

| Reason | Refused declaration |
| --- | --- |
| `graphs.ViewRootNotANode` | A root that is not a node of the graph |
| `graphs.ViewIsAlsoSchema` | A view type that is also a node or an edge |
| `graphs.DuplicateViewName` | Two views with the same name |
| `graphs.UnknownNodeProperty` | A property member the root node does not declare |
| `graphs.UnknownEdge` | A relation along an edge the graph does not declare |
| `graphs.UnknownEdgeProperty` | An edge property member the edge does not declare |
| `graphs.EdgeNotIncidentOnRoot` | An edge that does not touch the root of the view, or a direction that does not fit it |
| `graphs.AmbiguousDirection` | A self edge without `Outgoing` or `Incoming` |
| `graphs.TargetLabelMismatch` | A target view not rooted at the far end of the edge |
| `graphs.RelationTargetNotAView` | A relation target that was not added to the graph with `WithViews` |
| `graphs.RecursiveView` | A view that contains itself, directly or through other views |
| `graphs.EdgePropertyViewNotUsedAsTarget` | A view with edge property members that no relation targets |
| `graphs.EdgePropertyViewSharedAcrossEdges` | A view with edge property members that relations along more than one edge type target |

The compiled graph exposes the views in the canonical definition format that every
Chronicle client emits: `Definition.Views()` on the definitions in the client's
artifacts, or `Declaration.ViewDefinition(policy)` for a declaration. `Canonical()`
serializes it as indented JSON with a fixed property order. The definition is a
conformance and transport format, not something you author.

This client declares and validates views and describes them; it does not execute
them. Reading the graph through a view needs a host stack that runs the queries,
and none is provided for Go yet. Until then, read the graph with Cypher or the
tooling of the graph database, as described above.

## Run it

With a development Chronicle kernel running on port 35000:

```sh
go run ./examples/graphs
```

Expected output:

```text
Organization graph: 3 events projected
```

The example uses the in-memory graph, appends three events and waits until the
graph's observer reports them handled, with
[`Observers().Get`](../operations.md#read-observer-state-and-failures).
Registration is eventually consistent like every projection: a successful append is
not a sign that the graph has caught up.

## Refusals

Every refusal is a `*graphs.DeclarationError` that also matches
`chronicle.ErrInvalidConfiguration`. Use `errors.Is` with these sentinels:

| Sentinel | Refused declaration |
| --- | --- |
| `graphs.ErrNodeKey` | A node without exactly one key property |
| `graphs.ErrEdgeEndpoints` | An edge whose source or target is not a node of the graph |
| `graphs.ErrEdgeIdentity` | An edge with more than one key, or an event that cannot supply the identity |
| `graphs.ErrNestedObject` | A property that is an object or a list of objects; model it as a node of its own and connect it with an edge |
| `graphs.ErrUnknownProperty` | A key or mapping naming a property that the struct or the event does not have |
| `graphs.ErrInvalidMapping` | A duplicate or incompatible mapping, or a literal the kernel cannot read |
| `graphs.ErrDuplicateElement` | Two nodes or edges with the same label or type |
| `graphs.ErrElementIsReadModel` | A node or edge type that is also registered as a read model |
| `chronicle.ErrNotRegistered` | An event that is not registered with the client |

`Build` reports what does not depend on the naming policy and the registered
events. The rest is reported by `NewClient`. Events that come from another event
store are not supported by graphs.
