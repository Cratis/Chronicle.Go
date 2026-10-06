---
title: Projection variants
description: Model mutually exclusive lifecycle shapes with entering events and shared handlers.
---

Use variants when an entity changes shape during its lifecycle: a work item can
be a backlog item or a pull request, rather than one model with many nullable
fields. Each variant has its own read model. Entering one removes the same event
source from its siblings. A normal update cannot create an inactive variant.

## Model-bound variants

Register the events and the two read models below before constructing the client.
`WorkItem` is only a group marker; do not register it. `SharedWorkItem` supplies
mappings, not a separately registered read model.

These snippets use `events`, `projections` and `readmodels` from
`github.com/cratis/chronicle.go`. The complete registration and equivalent fluent
example are in [variants.go](../../examples/projections/variants.go).

```go
type WorkItem struct{}
type IssueOpened struct { Title string `json:"title"` }
type PullRequestOpened struct { URL string `json:"url"` }
type TitleChanged struct { Title string `json:"title"` }

type BacklogItem struct {
    ID string `json:"id" chronicle:"key"`
    Title string `json:"title"`
}
type PullRequestItem struct {
    ID string `json:"id" chronicle:"key"`
    Title string `json:"title"`
    URL string `json:"url"`
}
type SharedWorkItem struct {
    Title string `json:"title" chronicle:"set(TitleChanged)"`
}
```

Pass each returned declaration to `registry.AddProjection`. The model handles
must come from `chronicle.RegisterReadModel` on that registry; the event handles
must come from `chronicle.RegisterEvent`. Also register `TitleChanged`, which the
global tag resolves by its Go type name.

```go
func modelBoundVariants(backlog readmodels.Model[BacklogItem], pullRequest readmodels.Model[PullRequestItem], opened events.Type[IssueOpened], submitted events.Type[PullRequestOpened]) ([]projections.Declaration, error) {
    shared, err := projections.Global[SharedWorkItem](projections.GlobalFor[WorkItem]())
    if err != nil { return nil, err }
    return []projections.Declaration{
        projections.ModelBound(backlog, projections.VariantOf[WorkItem](), projections.EntersOn(opened)),
        projections.ModelBound(pullRequest, projections.VariantOf[WorkItem](), projections.EntersOn(submitted)),
        shared,
    }, nil
}
```

`NewClient` collects the complete group before compiling its definitions. Every
variant needs an explicit `key` field and at least one `EntersOn` event. Register
all variants in the same store registry; names alone never join separate groups.

A `TitleChanged` updates the active variant through a self-join. It cannot create
a backlog item after the entity has left the backlog. Re-entering is deliberate:
append `IssueOpened` again. Joins still run after local mappings, so the latest
shared title overrides the entering event's title, including on re-entry.

### Shared-handler boundaries

`GlobalFor` copies only **From property mappings**, in registration order. A later
shared write replaces an earlier local/shared write and produces a diagnostic.
It does not copy keys, joins, children, removals, Every/All mappings or settings.
Every shared target must exist on every variant; otherwise startup fails with a
`DeclarationError` wrapping `GlobalHandlerPropertyNotOnVariant`.

`EntersOn(event, projections.UsingKey(...))` can redirect the entering key.
Sibling removal nevertheless uses **event-source identity**, matching C#.
Choose consistent source identities across the lifecycle; a redirected entry key
is not automatically used to remove its siblings. An explicit `RemovedWith` for
the sibling event takes precedence over the generated removal.

## Fluent variants

The same read models can use the fluent builder because they carry no mapping
tags. Identity tags remain valid. Without a `key` tag, supply
`VariantKey(projections.Path[Model, string]("id"))` alongside `VariantOf`.
C# fluent authoring repeats shared `From` mappings on the variants:

```go
func fluentVariants(backlog readmodels.Model[BacklogItem], pullRequest readmodels.Model[PullRequestItem], opened events.Type[IssueOpened], submitted events.Type[PullRequestOpened], renamed events.Type[TitleChanged]) ([]projections.Declaration, error) {
    // Explicit shared From mappings are the C# fluent equivalent of GlobalFor.
    first := projections.NewBuilder("", backlog, projections.VariantOf[WorkItem](), projections.EntersOn(opened))
    projections.From(first, renamed, func(from *projections.FromBuilder[BacklogItem, TitleChanged]) {
        projections.Map(from, projections.Path[BacklogItem, string]("title"), projections.Path[TitleChanged, string]("title"))
    })
    second := projections.NewBuilder("", pullRequest, projections.VariantOf[WorkItem](), projections.EntersOn(submitted))
    projections.From(second, renamed, func(from *projections.FromBuilder[PullRequestItem, TitleChanged]) {
        projections.Map(from, projections.Path[PullRequestItem, string]("title"), projections.Path[TitleChanged, string]("title"))
    })
    a, err := first.Build()
    if err != nil { return nil, err }
    b, err := second.Build()
    if err != nil { return nil, err }
    return []projections.Declaration{a, b}, nil
}
```

Go also accepts a fluent shared-mapping builder with `GlobalFor[I]()`: use
`readmodels.Define[Shared]()` locally, build it, and add its declaration without
registering its model. Both forms use the same group compiler and encoder.

Run the example's declaration and equivalence checks without a server:

```sh
go test ./examples/projections -run TestVariantExamplesCompileAndMatch
```

## Every, All and registration

`every` and `all` still merge into the ordinary All dictionary. They do not
become derivative groups or shared `GlobalFor` mappings during variant lowering.
`every` stamps existing subscriptions. **Avoid `all` for an exclusive lifecycle**:
it subscribes to unrelated events too, and the kernel's all-event fallback can
create a model for those events. Variant update-only guarantees apply to the
explicit non-entering handlers reclassified as joins, not that broad fallback.

Each variant registers independently, but its definition already includes sibling
removals. Reconnect reuses those frozen definitions without reflective discovery.
`Definition.Hash()` fingerprints the finalized wire shape, including nested
nodes, exclusions and sibling removals. This Go SHA-256 convenience fingerprint
is stable only within one build: protobuf deterministic encoding can change across
binaries or dependency versions. Do not persist it across upgrades. Neither the
kernel nor C# uses a client hash.

## Recursive children

Concrete recursive structs are supported in schemas and projection declarations.
A `children` field can refer to its own element type. The compiler emits one
repeated node and stops further structural expansion on that traversal path;
a sibling collection of the same type is not suppressed. This is finite C#
model-bound expansion, not arbitrary-depth recursive event routing.

Keep type-level child options in `WithNodes(Node[Child](...))`. At a repeated
node, its own creator remains subscribed. Ancestor creators are excluded;
keyed update subscriptions propagate only when they also specify a parent key.
Each node keeps its own AutoMap exclusions and identity mapping. Fluent authoring
spells out the finite child structure and its identity mappings explicitly.
The [kernel fixture](../../internal/integration/projection_recursive_test.go)
shows a tree, branch and child with keyed updates.

Serialization uses schema references, rejects cyclic values, and bounds JSON
encoding to 256 levels. Interface/derived child codecs and `_derivedTypeId`
inference are not implemented: interface-valued children still fail declaration.
See the [per-attribute parity map](../parity.md#projections-part-3).

## Source stores and ad-hoc queries

`events.WithSourceStore("orders")` declares an event's origin. Without an explicit
sequence, a projection reads `event-log` in that store and `inbox-orders` elsewhere.
Inference includes root, child, join and removal handlers after variant lowering;
incompatible declared origins fail startup. `WithEventSequence`/`WithEventLog`,
including explicit model settings, override inference. Registration provisions
subscriptions for inbox sequences after all observers and before seeding. Like
C#, even an explicitly selected `inbox-<store>` provisions. See
[external integrations](../integrations/index.md).

For an ad-hoc PDL query, use `store.QueryProjection(ctx, declaration)` or its
historical alias `PreviewProjection`. An optional sequence defaults to
`events.EventLog`. On kernel 19.29.4, use an explicit target: inferred-schema
previews can drop projected properties
([Chronicle#4539](https://github.com/Cratis/Chronicle/issues/4539), fixed in 19.32.1).
Assuming a read model registered with identifier `PreviewIssue` and a `title`
property, this declaration queries its shape without registering a projection:

```text
projection PreviewIssues => PreviewIssue
  from IssueOpened
    title = title
```

The kernel executes the declaration; no Go callback runs and no permanent
projection is registered. Results contain JSON strings in `ReadModelEntries` and
the schema when returned. Omitting `=> PreviewIssue` requests kernel inference,
but that path remains Partial until the linked kernel defect is fixed.
Use `errors.As` with `*projections.QueryError` to inspect
line/column diagnostics. Set a context deadline. The pinned kernel processes at
most 1,000 matching events, so this is bounded exploration, not a complete query
API for large histories or a replacement for materialized read models.
