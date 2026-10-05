---
title: Seed reference events
description: Register global and namespace-specific seeds without duplicate startup appends.
---

Use event seeding for stable reference data that should exist when your application
starts. Register a seeder instead of calling `Append` at startup: the kernel tracks
seed entries across client restarts. Ordinary, unprotected events do not require
PII support.

You need registered [event types](event-types.md) and a running Chronicle kernel.
Global seeds reach existing namespaces; applying them to namespaces created later
has a [known kernel limitation](../parity.md#event-seeding).

## Declare and register seeds

This excerpt comes from the [runnable example](../../examples/seeding/main.go).
It uses the root `chronicle` package and the `events` and `seeding` packages under
`github.com/cratis/chronicle.go`.

```go
type ProductAdded struct {
    Name string
}

type CatalogSeeds struct{}

func (CatalogSeeds) Seed(builder *seeding.Builder) error {
    seeding.For(builder, "catalog-1", ProductAdded{Name: "Notebook"})
    seeding.For(builder.ForNamespace("demo"), "demo-1", ProductAdded{Name: "Demo notebook"})
    return nil
}

func declarations() (*chronicle.Registry, error) {
    registry := chronicle.NewRegistry()
    if _, err := chronicle.RegisterEvent[ProductAdded](registry,
        events.WithID("seeding-example-product-added"), events.WithTags("reference-data")); err != nil {
        return nil, err
    }
    if err := chronicle.RegisterSeeder(registry, CatalogSeeds{}); err != nil {
        return nil, err
    }
    return registry, nil
}
```

Pass this registry to `chronicle.WithRegistry` when constructing your client.
`NewClient` invokes each seeder once per registry snapshot, validates its events
against that store's catalog, and serializes them with the client's naming policy.
It does not connect. Callbacks must be synchronous, perform no I/O, and not retain
the builder. Invalid values, unknown event types, callback failures and
serialization failures stop construction; no partial batch is registered.

- `seeding.For[E](builder, source, events...)` adds one event type.
- `builder.ForEventSource(source, events...)` accepts heterogeneous registered
  event values. Non-nil event pointers work in both forms.
- The root builder is **global**, not scoped to the store handle's namespace.
  `ForNamespace(ns)` returns an independent scoped view. Explicitly selecting
  `chronicle.DefaultNamespace` seeds only that namespace.
- Static `events.WithTags` labels are included. Event source IDs are preserved.
- `RegisterSeederFunc(registry, func(*seeding.Builder) error)` is the plain-function
  alternative. `seeding.Func` also implements the seeder interface.
- Duplicate concrete seeder types fail. Plain-function registrations accumulate
  in order. `WithRegistryForStore` replaces default seeds along with the catalog.

Fluent builder errors are retained even when a seeder returns nil. Mutable input
values are serialized when added; later changes cannot alter the snapshot.

## Register and restart

`EventStore`, `Ready` and `WaitForRegistration` include seeding **last**, after event
types, read models, constraints, projections, and reactor/reducer registration
sends. Reactor and reducer streams have no server registration acknowledgement;
this ordering does not invent one. See [lifecycle and readiness](../connection-strings/lifecycle.md).

A successful send consumes pending work for that connection generation. Reconnect
resends the same immutable snapshot without invoking seeders or constructors
again. Concurrent callers share one store-wide seeding pass. Transport and command
envelope failures propagate through `RegistrationError`; transient failures use
the bounded registration retry policy. No ordinary append RPC is involved.

The kernel owns deduplication. Two identical entries in one declaration represent
two facts; Go does not discard either. Changing payloads, source IDs, event IDs or
tags can describe new seed entries, not updates to previously appended events.
Keep declarations stable across startups.

The seeding protocol carries event IDs but **no generation**; the kernel appends
seeds as generation one, like C#. Use generation-one-compatible seed shapes. The
protocol also has no per-entry append-result response: command acknowledgement is
not proof that every seed passed constraints. Consult kernel diagnostics and read
back required data; see the [parity boundaries](../parity.md#event-seeding).

## Construct seeders with optional services

Borrowed instances registered with `RegisterSeeder` remain caller-owned. For
owned construction use `RegisterSeederFactory[S](registry, constructor)`. The
constructor returns `S` or `(S, error)` and can accept a context followed by
service dependencies. A nil constructor selects registered-service or zero-value
activation. Plain Go construction needs no container.

Opt into Fundamentals.Go scopes through `services.WithServices(provider)`. Each
seeder gets a preparation scope: open, construct, declare, dispose the constructed
seeder, close the scope. Registered services take precedence over constructors;
the provider retains singleton ownership. Cleanup errors fail preparation, as do
activation errors; unlike C# discovery, Go does not log and omit a failed seeder.
Use `NewClientContext(ctx, options...)` for cancellation during scoped preparation.
The client never retains that context or the operation scope.

## Offer a manual batch

Call `store.PrepareSeeds(seeding.Func(...))` to build a `*chronicle.SeedBatch` from
that store's catalog without sending it. Then call `batch.Register(ctx)`. The
batch waits for the store's observers, retains entries after failure and consumes
pending work only after success. Retrying it never reruns the callback; repeated
successful calls do nothing. Prepare a new batch when correcting definitions.
Unscoped manual entries are still global.

## Run the example

From the repository root, against a local development kernel:

```sh
CHRONICLE_CONNECTION_STRING=chronicle://localhost:35000 go run ./examples/seeding
```

The example creates the `go-seeding-example` store and `demo` namespace. It uses
development credentials and accepts the development certificate; do not copy
those connection settings into production. Running it again prints the same
counts:

```text
global=1 demo-only=1
```

Use a different example store name for a fresh history rather than deleting
shared kernel data.
