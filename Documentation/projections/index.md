---
title: Projections
description: Declare model-bound or fluent mappings and read kernel-materialized state.
---

Use a projection to populate a read model from events. Chronicle runs the mappings;
your Go process declares them and reads the result. Start with model-bound tags.
Use the fluent builder when keeping the mappings separately makes the model clearer.
Both forms support scalar mappings, arithmetic, global mappings, children, event
joins, nested objects and removals. See [orders with children and joins](orders.md)
for their model-bound and fluent equivalents. See [variants](variants.md) for
mutually exclusive lifecycle shapes, shared handlers, recursive children and
ad-hoc projection queries. See [initial values and labels](defaults.md) for
initial model state and artifact metadata.

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
| `context(E,from=occurred)` / `context(E,from=Tags)` | Assign an admitted kernel EventContext property; whole Tags supports string slices; also subscribes to E |
| `value(E,value="active")` | Assign a typed JSON scalar literal |
| `value(E,value=null)` / `clear(E)` | Clear a nullable scalar or direct compiled slice/string-keyed map pointer with `$null` |
| `add(E,from=amount)` / `subtract(E,from=amount)` | Numeric running total; omitted `from` uses this field's serialized name |
| `increment(E)` / `decrement(E)` / `count(E)` | Kernel `$increment`, `$decrement`, `$count`; optional `key=value("total")` |
| `every(from=name)` / `every(context=occurred)` | Map existing subscriptions without discovering events |
| `all(from=name)` / `all(context=occurred)` | Merge global mappings and subscribe to all events |
| `children(E,key=itemId,identified-by=id,parent-key=orderId)` | Project a collection node; omitted keys invoke identity conventions |
| `join(E,on=customerId,from=name)` | Enrich from an event after local mappings; omitted paths use the target name |
| `nested;clear(E)` | Project a pointer-to-object node and remove it on E |
| `remove(E,key=itemId,parent-key=orderId)` | Remove a collection child, or the containing instance on other fields |
| `remove-join(E,key=itemId)` | Distinct join-removal contract; see [kernel limits](orders.md#joins-globals-and-removals) |
| `key` | Mark model identity metadata; does not redirect event correlation |
| `no-auto` | Exclude this field from its node's AutoMap; explicit mappings still work |
| `not-projected` | Exclude from AutoMap and record the intentionally unmapped field |

Inside a Go tag, escape JSON string quotes as `\"`. Commas and semicolons inside
quoted strings are parsed as literal contents, not separators. The kernel's
`$value(...)` grammar cannot represent every JSON string: commas, semicolons,
parentheses, quotes, newlines and characters above U+FFFF are rejected rather than
misencoded. Property paths exactly equal to `true`, `True`, `false` or `False` are
also rejected: the kernel resolves those as boolean literals, not event fields. `null`
is not an empty string or zero value; use pointers for nullable scalars.
Whole context `Tags` (or `tags`) maps to a slice of non-nullable, unformatted
strings, including `[]events.Tag`, `[]string` and their pointer forms. It preserves
order and maps empty tags to `[]`, independently of any conflicting payload field.
For example, a registered model field can declare:

```go
Labels *[]events.Tag `json:"labels" chronicle:"no-auto;context(AuditStamped,from=Tags)"`
```

The equivalent fluent write inside an `AuditStamped` subscription is
`projections.Context(from, projections.Path[Audit, *[]events.Tag]("labels"), "Tags")`.
The model and event must already be registered. `no-auto` and `not-projected`
exclude AutoMap only; explicit context and payload assignments still work.
Every/All context mappings use the same validation. Context keys remain scalar-only.
Other complex properties (`CausedBy`, `EventType`, `NamedTags`, `Causation`) fail
with a located `ErrUnsupported`; unknown paths and incompatible types remain
configuration errors. Paths are not arbitrary functions, indexes or expressions.

Startup projections admit `clear`/`Clear` and
`value(...,value=null)`/`Value(...,nil)` on direct compiled pointers to slices and
string-keyed maps, as well as nullable scalar pointers. All emit `$null`.
The ordinary Go pointer-container profile preserves **typed nil versus empty**:
after a processed clear, the document survives and its collection pointers are nil,
not restored to schema defaults, stale values, `[]` or `{}`. Materialized raw reads
may omit the properties or carry explicit JSON null; the pinned kernel omits them.
No missing-to-null overlay is added. Present raw null is a stronger, separate
capability, not required for this typed profile.

This follows C# nullable collection behavior for the ordinary no-initializer
profile, not universal equivalence for constructors/default property initializers
or configured serializers. Bare slices/maps are not nullable declarations; fixed
arrays, interfaces, structural objects and collection-element paths are not direct
clears. Existing structural child/nested removals are unchanged. Runtime
`RegisterProjection` still refuses collection models before publication/RPC; use
the startup registry. Other complex context sources remain
[partial](../parity.md).

Pointers preserve typed nil versus empty collections, but cannot distinguish
missing JSON from explicit null: both decode to a nil pointer. Model serialization
omits both nil outer pointers and pointers to nil slices/maps; pointers to empty
collections encode `[]`/`{}`. A default projection request is still `{}` and
registration creates no instance. That request is not evidence that stored
properties remain absent: kernel initial-state construction seeds array-schema
properties with empty arrays.

Arithmetic embeds paths in a stricter kernel regex: ASCII letters/digits and dots,
with an underscore permitted only as the first character. Even a legal JSON
property such as `amount_delta` cannot be used inside `$add`/`$subtract`.

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
`UsingParentKey(Path[E,V]("parentId"))`, `UsingConstantKey("total")`,
`UsingKeyFromContext("eventSourceId")` and `UsingParentKeyFromContext("namespace")`.
Use `UsingCompositeKey(func(*CompositeKeyBuilder[K,E]))` with ordered `KeyPart`,
`KeyPartFromContext`, `KeyPartFromSource` or `KeyPartValue` calls. Target parts use
K's serialization plan (default spelling; explicit JSON tags recommended).
`UsingCompositeParentKey` supplies the same form for a parent key. Tag keys accept
`source`, `context(path)`, `value(scalar)` and
`composite(part=path,other=context(namespace))`. Duplicate parts and nested
composites fail because the kernel cannot represent them faithfully.
The default key is `$eventSourceId`; parent key defaults to unset. Key settings
are last-wins, so a later constant key overrides an earlier property key.

`ModelBound` and `NewBuilder` accept these shared options:

| Option | Default and effect |
| --- | --- |
| `WithIdentifier(id)` | Model observer ID when set, otherwise full Go model type name; choose a stable C# name for shared definitions |
| `WithEventSequence(id)` / `WithEventLog()` | Explicit selection overrides source-store inference; model reads use the same sequence |
| `NoAutoMap()` / `AutoMap()` | Enabled by default |
| `NotRewindable()` | Rewindable by default |
| `Passive()` | Active by default; passive disables observation and selects the `None` sink for immediate reads |
| `WithInitialValues(value)` / `WithInitialValue(path,value)` | `{}` by default; typed model/scalar snapshots, including explicit scalar nulls |
| `WithLabels(labels...)` | No labels by default; copied, first-occurrence deduplicated artifact metadata, never event filters |

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
is `set`, `add`, `subtract`, `increment`, `decrement`, `count`, `context`,
`value`, then `clear`; the last directive within a family wins.
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
registry after `NewClient` does not alter that client. Definitions expose a
`Hash()` of their finalized wire shape, stable only within one build. Do not
persist it across upgrades; neither the kernel nor C# uses a client hash. Runtime
registry extension remains unimplemented. Children of a single-derivative family
are covered in [derived children](../derived-codecs.md#project-derived-children). Inbox sequences now
[provision external subscriptions](../integrations/index.md); see
[variants, recursive children and queries](variants.md) and the
[attribute parity map](../parity.md) for the remaining boundaries.
