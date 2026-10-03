---
title: Project orders with children and joins
description: Combine model-bound child collections, numeric totals and event joins, or use the equivalent fluent declarations.
---

Use child projections for an order's lines, arithmetic for its running total, and
an event join for the customer's latest name. Register the event and root model
types before constructing the client, as in the [projection guide](index.md).
The excerpts below come from [the compiled example](../../examples/projections/orders.go).

## Events

Append `OrderPlaced` and `LineAdded` to the order's event source. Append
`CustomerRenamed` to the customer's source. `customerId` joins that source;
`orderId` selects the parent document and `lineId` identifies a collection item.

```go
type OrderPlaced struct {
    CustomerID string `json:"customerId"`
}

type LineAdded struct {
    OrderID string `json:"orderId"`
    LineID string `json:"lineId"`
    Name string `json:"name"`
    Amount int32 `json:"amount"`
}

type CustomerRenamed struct {
    Name string `json:"name"`
}
```

## Model-bound first

Register `Order` as the read model; `Line` is a reachable node, not a separately
registered model. Pass the returned declaration to `registry.AddProjection`.
`FromEvent(placed)` enables AutoMap for `CustomerID`. `add`, `children` and `join`
subscribe to their respective events without further root subscriptions.

```go
type Line struct {
    ID string `json:"id" chronicle:"key"`
    Name string `json:"name"`
}

type Order struct {
    ID string `json:"id" chronicle:"key"`
    CustomerID string `json:"customerId"`
    CustomerName string `json:"customerName" chronicle:"join(CustomerRenamed,on=customerId,from=name)"`
    Total int32 `json:"total" chronicle:"add(LineAdded,from=amount)"`
    Lines []Line `json:"lines" chronicle:"children(LineAdded,key=lineId,identified-by=id,parent-key=orderId)"`
}

func modelBoundOrder(model readmodels.Model[Order], placed events.Type[OrderPlaced]) projections.Declaration {
    return projections.ModelBound(model, projections.WithIdentifier("orders"), projections.FromEvent(placed))
}
```

An appended line with `amount: 4` adds four to `Total` and creates or updates the
line identified by `lineId`. A later customer rename updates `CustomerName`.
Read the order by its event-source ID; active projection reads are eventually
consistent. The [kernel specification](../../internal/integration/projection_nodes_test.go)
also demonstrates removal, nested clears, scalar clears and global timestamps.

## Fluent equivalent

Use the same event handles with an untagged mapping model. `Line` retains its
identity tag. Each callback is executed once and copied; it is not a runtime
handler. Add the built declaration to the registry just as above.

```go
type FluentOrder struct {
    ID string `json:"id" chronicle:"key"`
    CustomerID string `json:"customerId"`
    CustomerName string `json:"customerName"`
    Total int32 `json:"total"`
    Lines []Line `json:"lines"`
}

func fluentOrder(model readmodels.Model[FluentOrder], placed events.Type[OrderPlaced], added events.Type[LineAdded], renamed events.Type[CustomerRenamed]) (projections.Declaration, error) {
    builder := projections.NewBuilder("orders", model)
    projections.From(builder, placed, nil)
    projections.From(builder, added, func(from *projections.FromBuilder[FluentOrder, LineAdded]) {
        projections.Add(from, projections.Path[FluentOrder, int32]("total"), projections.Path[LineAdded, int32]("amount"))
    })
    projections.Children(builder, projections.Path[FluentOrder, []Line]("lines"), func(child *projections.Builder[Line]) {
        projections.From(child, added, nil,
            projections.UsingKey(projections.Path[LineAdded, string]("lineId")),
            projections.UsingParentKey(projections.Path[LineAdded, string]("orderId")))
    }, projections.IdentifiedBy(projections.Path[Line, string]("id")))
    projections.Join(builder, renamed, projections.Path[FluentOrder, string]("customerId"), func(join *projections.FromBuilder[FluentOrder, CustomerRenamed]) {
        projections.Map(join, projections.Path[FluentOrder, string]("customerName"), projections.Path[CustomerRenamed, string]("name"))
    })
    return builder.Build()
}
```

Check these declarations without a kernel:

```sh
go test ./examples/projections
```

## Choose identities deliberately

If `identified-by` is absent, discovery tries the child's `key` field, a
case-insensitive Go `Id` field, a serialized property matching the event key,
then `$eventSourceId`. Go has no constructor-parameter attributes; the C# first
public constructor's `Key` step has no Go equivalent.

If `parent-key` is absent, discovery selects the first event field whose
**declared type** equals the parent's Go `ID` type, excluding the child key.
Multiple matches retain C# declaration-order precedence and produce a diagnostic.
Distinct domain ID concepts are not equated through their underlying strings.
Use explicit keys when multiple string or UUID properties could match.

Child-only subscriptions remain child-scoped. Parent `NoAutoMap()` propagates to
children and nested objects; exclusions remain local. Use
`WithNodes(Node[Line](FromEvent(updated, ...)))` for child type-level options and
`Node[Address](ClearWith(cleared))` for a reusable nested clear. Aliases and
`WithNodes` belong on the root. No separate read-model registration is needed.

## Joins, globals and removals

- Joins consume **events**, not other read models. Their mappings run after local
  writes. A local clear cannot reset a joined property; overlap diagnostics flag
  this. Model-bound joins for the same event share the first `on`; fluent duplicate
  join declarations fail. Go accepts explicit child `on` paths through both front
  ends, normalizing the C# fluent/model-bound authoring difference.
- `every(from=name)` and `every(context=occurred)` map already-subscribed events.
  `all(...)` also enables subscription to all events. Both merge into contract
  `All`, not the `FromEvery` derivative-group field. Fluent equivalents are
  `Every`/`All` with `EveryMap` or `EveryContext`.
- Model-bound globals share bare target names across nodes, like C#. Collisions
  produce diagnostics; the last mapping wins. `IncludeChildren` defaults to true.
  The pinned kernel does **not** run parent Every mappings for child-only events;
  that flag is preserved, not a stronger runtime guarantee.
- `clear(E)` on a nullable scalar emits `$null`. A `nested;clear(E)` pointer field
  removes the nested object instead. Fluent `Clear` and `Nested` with `ClearWith`
  express the same distinction. Collection clears are rejected; remove items.
- `remove(E,key=lineId,parent-key=orderId)` beside `children(...)` removes that
  child, including on grandchild collection properties. On other fields it removes
  the containing instance. Root `RemovedWith` and fluent child `Configure` accept
  the same key options. Removal parent keys default to source, not inference.
- `remove-join` and `RemovedWithJoin` preserve the C# contract. In Chronicle
  19.29.4-development, the MongoDB sink does **not** remove children identified by
  `id` or `Id`: it stores those names as `_id` but does not translate the join-removal
  filter ([Chronicle#4538](https://github.com/Cratis/Chronicle/issues/4538)). This
  affects both event-source and event-property removal keys, with or without a
  prior remove/re-add. A child identified by `GroupId`, as in the C# integration
  scenario, does work. Do not rewrite client definitions to MongoDB's `_id` name;
  the API and wire encoding remain sink-independent.
  The kernel explicitly does **not** support root join removals
  ([Chronicle#4263](https://github.com/Cratis/Chronicle/issues/4263)) or nested-object
  join removals (diagnosed in
  [Chronicle#4125](https://github.com/Cratis/Chronicle/issues/4125)). It also does not
  execute children inside nested objects, or nested-object joins inside collection
  children. Avoid these shapes; successful definition registration is not evidence
  they will materialize.

Recursive and polymorphic child schemas, variants and derivative groups remain
in [part 3](https://github.com/Cratis/Chronicle.Go/issues/27).
