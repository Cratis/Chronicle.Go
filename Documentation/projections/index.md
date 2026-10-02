---
title: Projections
description: Declare model-bound or fluent mappings and read kernel-materialized state.
---

Use a projection to populate a read model from events. Chronicle runs the mappings;
your Go process declares them and reads the result. Start with model-bound tags.
Use the fluent builder when keeping the mappings separately makes the model clearer.
Both forms support basic mappings today; children, arithmetic, joins and variants
remain in the [projection series](https://github.com/Cratis/Chronicle.Go/issues/24).

## Model-bound declarations

These declarations come from the [runnable example](../../examples/projections/main.go).
They use `chronicle`, `events`, `projections`, `readmodels`, and standard `time`
imports. Call `declarations()` before constructing the client.

```go
// ProductRegistered records the initial product description.
type ProductRegistered struct {
    ProductName string `json:"productName"`
    Description string `json:"description"`
}

// Inventory is maintained by Chronicle, not by an in-process event handler.
type Inventory struct {
    ID string `json:"id" chronicle:"key"`
    ProductName string `json:"productName"`
    Description string `json:"description" chronicle:"set(@registered)"`
    State string `json:"state" chronicle:"value(@registered,value=\"available\")"`
    Registered time.Time `json:"registered" chronicle:"context(@registered,from=occurred)"`
}

func declarations() (*chronicle.Registry, readmodels.Model[Inventory], error) {
    registry := chronicle.NewRegistry()
    registered, err := chronicle.RegisterEvent[ProductRegistered](registry, events.WithID("product-registered"))
    if err != nil {
        return nil, readmodels.Model[Inventory]{}, err
    }
    model, err := chronicle.RegisterReadModel[Inventory](registry)
    if err != nil {
        return nil, model, err
    }
    projection := projections.ModelBound(model, projections.BindEvent("registered", registered), projections.FromEvent(registered))
    return registry, model, registry.AddProjection(projection)
}
```

`ProductName` maps automatically: **AutoMap is enabled by default**. `set`,
`context` and `value` each subscribe to their referenced event, even without a
`FromEvent` option. A context-only subscription also enables payload AutoMap.
`FromEvent` is useful for subscribing without any property mappings or selecting
an event key.

Register model types explicitly. `NewClient(chronicle.WithRegistry(registry))`
automatically discovers mapping tags on those models. You only need
`AddProjection(ModelBound(...))` for aliases or type-level settings. `key`,
`no-auto`, and `not-projected` alone do not discover a projection.

## Fluent declarations

The alternative below uses the same `ProductRegistered` event. Keep property
mapping tags off the fluent model; mixing the two sources is an error. Identity
and exclusion tags still apply. AutoMap remains enabled, so no explicit mapping
for `ProductName` is needed.

```go
// FluentInventory uses the same mappings without model-bound mapping tags.
type FluentInventory struct {
    ID string `json:"id" chronicle:"key"`
    ProductName string `json:"productName"`
    Description string `json:"description"`
    State string `json:"state"`
    Registered time.Time `json:"registered"`
}

func fluentDeclarations() (*chronicle.Registry, readmodels.Model[FluentInventory], error) {
    registry := chronicle.NewRegistry()
    registered, err := chronicle.RegisterEvent[ProductRegistered](registry, events.WithID("product-registered"))
    if err != nil {
        return nil, readmodels.Model[FluentInventory]{}, err
    }
    model, err := chronicle.RegisterReadModel[FluentInventory](registry)
    if err != nil {
        return nil, model, err
    }
    builder := projections.NewBuilder("inventory", model)
    projections.From(builder, registered, func(from *projections.FromBuilder[FluentInventory, ProductRegistered]) {
        projections.Map(from, projections.Path[FluentInventory, string]("description"), projections.Path[ProductRegistered, string]("description"))
        projections.Context(from, projections.Path[FluentInventory, time.Time]("registered"), "occurred")
        projections.Value(from, projections.Path[FluentInventory, string]("state"), "available")
    })
    declaration, err := builder.Build()
    if err != nil {
        return nil, model, err
    }
    return registry, model, registry.AddProjection(declaration)
}
```

`Path[T,V]` checks both the serialized name and declared Go value type. `Map`,
`MapAs`, `Context`, `Value`, `EventSourceID`, and `From` are package functions, not generic
methods. Use `MapAs` when declared Go types differ but their serialized representations
are compatible, such as an event `string` mapped to a model `*string`. Supply each
field's actual type in `Path`; `Build` validates compatibility, not arbitrary conversions. Callbacks run once during authoring; reconnect never calls them again.
`Build` freezes the result and rejects duplicate writes for the same event/property.

## Field declarations

| Tag | Behavior |
| --- | --- |
| `set(E)` | Assign the event property matching this field's serialized name |
| `set(E,from=details.name)` | Assign an exact serialized event path |
| `context(E,from=occurred)` | Assign a kernel EventContext scalar; also subscribes to E |
| `value(E,value="active")` | Assign a typed JSON scalar literal |
| `value(E,value=null)` | Clear a nullable scalar pointer with `$null` |
| `key` | Mark model identity metadata; does not redirect event correlation |
| `no-auto` | Exclude this root field from AutoMap; explicit mappings still work |
| `not-projected` | Exclude from AutoMap and record the intentionally unmapped field |

Inside a Go tag, escape JSON string quotes as `\"`. Commas and semicolons inside
quoted strings are parsed as literal contents, not separators. The kernel's
`$value(...)` grammar cannot represent every JSON string: commas, semicolons,
parentheses, quotes, newlines and characters above U+FFFF are rejected rather than
misencoded. Property paths exactly equal to `true`, `True`, `false` or `False` are
also rejected: the kernel resolves those as boolean literals, not event fields. `null`
is not an empty string or zero value; use pointers for nullable scalars.

Paths come exclusively from the serialization plan: explicit `json` spelling wins.
The compiler does not recase paths. `json:"-"` and unexported fields cannot carry
declarations. Neither `omitempty` nor `omitzero` makes a scalar nullable.
Unsupported directives, including not-yet-implemented security metadata, fail
closed. Projection directives on event payload types are rejected.

### Event references

- `Opened`: a unique registered Go simple type name, not a persisted ID.
- `go("example.org/shop/events.Opened")`: an exact import-qualified Go type.
- `id("account-opened")`: the catalog's current persisted event identity.
- `id("account-opened",2)`: an exactly registered generation.
- `@opened`: a typed handle supplied by `BindEvent("opened", opened)`.

Only the frozen store catalog participates. Ambiguous names report qualified
candidates; there is no package loading, global discovery or guessed ID.
Go rename tools do not rewrite tag strings; aliases keep event renames in Go code.

## Keys and settings

`FromEvent(handle, options...)` and `From(builder, handle, callback, options...)`
accept `UsingKey(Path[E,V]("accountId"))`,
`UsingParentKey(Path[E,V]("parentId"))` and `UsingConstantKey("total")`.
The default key is `$eventSourceId`; parent key defaults to unset. Key settings
are last-wins, so a later constant key overrides an earlier property key.

`ModelBound` and `NewBuilder` accept these shared options:

| Option | Default and effect |
| --- | --- |
| `WithIdentifier(id)` | Model observer ID when set, otherwise full Go model type name; choose a stable C# name for shared definitions |
| `WithEventSequence(id)` / `WithEventLog()` | Model event sequence (default `event-log`); model reads use the same sequence |
| `NoAutoMap()` / `AutoMap()` | Enabled by default |
| `NotRewindable()` | Rewindable by default |
| `Passive()` | Active by default; passive disables observation and selects the `None` sink for immediate reads |

An explicit `NewBuilder` ID wins over `WithIdentifier`. Discovered projections also
inherit the model's observer ID and event sequence. Explicit conflicting producer
or sequence settings (including an explicitly selected `event-log`) and incompatible
passive sink settings fail construction. Passive key
redirection is rejected because immediate instance reads select by event source.
`key` does not create a mapping to an arbitrarily named root property: explicitly
map such a property with `context(E,from=eventSourceId)` or `EventSourceID`.

All declarations validate atomically at `NewClient`, before connection work.
Inspect `*projections.DeclarationError` with `errors.As` for artifact, Go field,
serialized path, directive and decoded tag byte offset. Error text omits literal
contents. `Definition.Diagnostics()` reports model-bound shadowing: C# precedence
is `set`, then `context`, then `value`; the last directive within a family wins.
Fluent duplicate writes are errors instead.

## Run and read the model

With a development Chronicle kernel running on port 35000:

```sh
go run ./examples/projections
```

Expected output:

```text
item-1: Notebook (available)
```

The example creates a uniquely named store, appends a product event, then polls
`readmodels.For(store.ReadModels(), model).Get(ctx, "item-1")` for up to 15 seconds.
Active projections are eventually consistent; successful registration or append
is not a sink-completion signal. Always check `Exists` before using `Value`.
The example uses development TLS settings and leaves its store in the development
kernel; use a disposable kernel for repeat runs, not production credentials/data.

Definitions register after event types and read models, share the store-level
registration barrier, and replay from frozen snapshots on reconnect. Adding to a
registry after `NewClient` does not alter that client. Runtime registry extension,
preview/query, initial-value authoring, recursive schemas and source-store inbox
inference remain outside this slice. See the [attribute parity map](../parity.md)
for the remaining boundaries.
