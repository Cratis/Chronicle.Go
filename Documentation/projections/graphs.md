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
or the tooling of the graph database. Graph projections need Chronicle 19.129.0
or later, and the client produces the same definition as the .NET client.

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
