---
title: Prepare definitions from configuration
description: Build immutable projections, constraints and event migrations with plain constructors or borrowed service providers.
---

Use a definition factory when configuration or an application service supplies a
projection, constraint, or migration definition. Keep direct declarations when no
construction is needed. This experimental v0.x API supports configuration-only
dependencies. [Captured client preparation](facade-composition.md) supports an
explicit borrowed client identity; automatic facade bindings are not provided.

## Register a config-backed projection

Register the event and read model first, then declare the projection's identity
and model before supplying its constructor. The constructor needs no container.
This excerpt uses `registry`, `opened`, `model`, and `projectionSettings` from the
[complete executable example](../example_definition_factories_test.go).

```go
settings := projectionSettings{Label: "orders"}
err = chronicle.RegisterProjectionFactory(registry, "orders", model.Descriptor(),
    func() projectionSettings { return settings },
    func(_ context.Context, config projectionSettings) (projections.Declaration, error) {
        return projections.ModelBound(model,
            projections.WithIdentifier("orders"),
            projections.FromEvent(opened),
            projections.WithLabels(config.Label)), nil
    })
if err != nil {
    panic(err)
}
client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
if err != nil {
    panic(err)
}
defer func() {
    if err := client.Close(); err != nil {
        panic(err)
    }
}()
```

Run `go test -run ExampleRegisterProjectionFactory .` in this repository. The
example prints `orders` from the prepared projection's labels without connecting
to a kernel. Connect the returned client to register its frozen definitions.

The same pattern applies to the other families:

- `RegisterConstraintFactory[C](registry, names, factory, define)` copies the
  complete name list. `define(ctx, C)` returns `([]constraints.Definition, error)`
  using the existing constraint builders. Return exactly one definition per name.
- `RegisterEventMigrationFactory[M](registry, upgrade, previous, factory, define)`
  takes the original registered event descriptors. `define(ctx, M)` returns
  `(events.MigrationDeclaration, error)` using `events.DefineMigration` with typed
  handles. Its directional builders produce frozen transformations, not callbacks
  retained for event processing.

Projection output must match both the declared ID and the original registered
model descriptor. Migration output must match both original endpoint descriptors.
Duplicate direct/factory IDs, models, constraint names, and migration edges are
errors, not replacement rules. Model-bound constraints cannot share factory names;
use `ConfigureDeclaredConstraint` to compose those instead. Factory projections
produce registered models; direct global declarations can participate in the same
variant group. `Registry.WithProjection` removes a replaced factory producer too.

## Use optional services without transferring ownership

Pass `services.WithServices(provider)` to `NewClientContext` for Fundamentals.Go
v0.1 scopes. Constructors return `P` or `(P, error)` and may take a leading
`context.Context` followed by dependencies. A nil constructor selects service or
default activation. If the provider advertises the artifact type through its
Catalog, that registered service takes precedence over an explicit constructor.
Without a container, nil activation supports zero-valued structs, pointers to
structs, and empty slices, just like observers and seeders.

Each definition factory has a temporary scope:

1. Chronicle opens the scope and constructs or resolves the artifact.
2. Your synchronous callback returns immutable authoring metadata.
3. Chronicle closes owned constructor results, then the scope, before reporting
   successful preparation. A nonnil constructor result returned with an error is
   still owned. A partially opened scope is still closed.

Resolved services belong to their scope or provider. Chronicle never closes them
separately or closes the provider. In particular, a resolved Singleton survives
preparation. Explicit constructors must not return borrowed aliases as owned
results. Close Chronicle and join any outstanding preparation before closing your
provider.

Cleanup is close-once per lease, in reverse acquisition order. `CloseContext(ctx)`
takes precedence over `Close(ctx)` and `Close()`. Cleanup preserves context metadata,
ignores caller cancellation, and supplies a cooperative five-second deadline;
a closer that ignores context cannot be forcibly interrupted. Borrowed resolvers
cannot close the scope and reject use after the callback lifetime.

## Respect the preparation boundary

`NewClient` captures every selected registry before any preparation callback.
`WithRegistryForStore`, including a nil registry, completely replaces defaults.
Shared registry pointers compile once per client, even across multiple stores.
All selected base schemas are naming-bound and classification results are frozen
before definition factories or constraint composition run. Original model handles
remain valid. Store binding only adjusts producer/source coordinates; reconnect
reuses the prepared registration requests without constructors or classifiers.

Callbacks and cleanup run outside registry/client locks and counted runtime work.
They must remain synchronous, avoid external effects, and not retain builders or
scoped collaborators. Separate clients may prepare the same declarations
concurrently; application configuration and codec callbacks must be deterministic
and safe for that use. A frozen plan cannot freeze mutable state hidden in a closure.

Factory constraints initially permit only static `WithMessage` templates.
`WithMessageProvider` is rejected because a bound method could outlive its
preparation scope. Direct constraints keep their existing runtime-provider
contract. Client-lifetime collaborators need a separate explicit API.

No factory may resolve its own not-yet-prepared event store or sequence. A factory
may borrow the exact `*Client` identity through an explicit binding after
`CaptureClient`; it cannot use the client's operational APIs until preparation
succeeds. Client values, default-fabricated clients and SDK facade artifact results
are rejected. Visible prohibited constructor dependencies fail before any scope
opens; hidden closure or container-internal dependencies remain your responsibility.

Preparation errors and panics return `PreparationError`. Its formatted text is
payload-free. Recovered panic values (strings, objects and errors) are immediately
discarded: they are neither formatted nor retained in the returned diagnostics.
There is no recovered-value accessor. Ordinary application failures remain
inspectable with `errors.Is`/`errors.As`.

The optional `services` adapter also sanitizes Fundamentals.Go provider errors.
It creates fresh `dependencyinjection.Error` diagnostics with copied type-key paths,
known operation names and stable failure categories. `Panic` is always nil, and a
panic-bearing node's original `Cause` is discarded, even if it is an error value.
Independent ordinary failures in a joined tree remain inspectable. Non-panic
provider causes retain plain error leaves (including typed errors and cancellation
sentinels), but application wrappers and their cached text are replaced, so wrapper
identity is not preserved. Custom `As`/`Is` hooks are neither called nor forwarded.
Unknown category/operation metadata is replaced with controlled diagnostics.

Error-tree inspection runs under a separate recovery boundary, never inside a
recovery defer. Cycles, more than 64 levels or 256 visited nodes, and unsupported
inspection hooks produce payload-free failure diagnostics. A panic in `Unwrap`
is discarded too; it cannot prevent cleanup of an already returned partial scope.
As with preparation callbacks, an application `Unwrap` must return synchronously;
these traversal limits cannot interrupt a blocking method.

Any preparation or cleanup failure returns no client and publishes no partial
local catalog. Application side effects already performed cannot be rolled back.

See [projections](projections/index.md), [constraints](events/constraints.md),
[event evolution](events/evolution.md), and [parity limits](parity.md#definition-factories).
