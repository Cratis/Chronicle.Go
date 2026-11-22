---
title: Seed reference events
description: Register global and namespace-specific event seeds so initial data is appended once at startup, without creating duplicate events on restart.
---

Use event seeding for stable reference data that should exist when your application
starts. Register a seeder instead of calling `Append` at startup: the kernel tracks
seed entries across client restarts. Ordinary, unprotected events do not require
PII support.

You need registered [event types](event-types.md) and a running Chronicle kernel.
Global seeds reach existing namespaces. Applying them to namespaces created later
requires the kernel's durable `NamespaceAdded` seeding path
([Chronicle#4547](https://github.com/Cratis/Chronicle/issues/4547)).
`TestKernelGlobalSeedsReachFutureNamespaces` is a required regression gate against
the CI kernel (`19.153.0-development`); runtime qualification remains pending until
that integration check passes. Older kernels without this path are not covered.
See the [seeding compatibility boundary](../parity.md#event-seeding).

## Declare and register seeds

This excerpt comes from the [runnable example](https://github.com/Cratis/Chronicle.Go/blob/main/examples/seeding/main.go).
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

## Target a stream

`WithStream(streamType, streamID)` returns an independent builder view. It works
with both `seeding.For[E]` and heterogeneous `ForEventSource` calls:

```go
seeding.For(builder.WithStream("Catalog", "published"), "item-1", ReferenceItemAdded{Name: "Notebook"})
builder.ForNamespace("demo").WithStream("Catalog", "draft").ForEventSource("item-1", ReferenceItemAdded{Name: "Draft notebook"})
```

The [compiled example](https://github.com/Cratis/Chronicle.Go/blob/main/example_seeding_streams_test.go)
registers these declarations. Select the namespace before or after the stream;
either order preserves both coordinates. Neither view changes its parent or
siblings. This is stream **type and ID** routing, not a non-default event-source
type API. Event source IDs and static tags remain unchanged.

Only empty strings default: an empty stream type becomes `events.AllStreamTypes`
(`All`), and an empty stream ID becomes `events.DefaultStreamID` (`Default`).
Whitespace is opaque and preserved verbatim, including whitespace-only values;
it is not trimmed or treated as a default. Builders that never use `WithStream`
keep routing omitted on the wire, preserving existing seed snapshots.

Before sending any batch containing non-default routing, the client calls
`GetSeedingSupport` and requires an authorized, successful envelope with present
`Data` and `RoutingSupported = true`. A false answer or an unimplemented RPC
returns an error matching `chronicle.ErrUnsupported`: upgrade the kernel or use
default routing. Missing data fails with `ErrProtocol`; authorization, validation,
transport, cancellation and deadline failures propagate without sending seeds.
Legacy and explicitly default-only batches do not query this capability. There
is no fallback to the default stream, even for a mixed legacy/routed batch. The
check runs again for each pending send, including retries and reconnects.

Stream type and ID participate in kernel seed identity alongside source ID,
event type, exact content bytes and the tag set. Equal payloads targeting two
streams are distinct entries; repeated equal entries in one declaration remain
repeated facts. Changing a route describes a new entry, not a move or update of
an existing event. Go performs no local deduplication.

Runtime qualification of stream-targeted seeding is **pending CI** against the
pinned `19.153.0-development` kernel. The required
`TestKernelStreamSeedsCSharpThenGoRestartAndNamespaceIsolation` witness checks
both streams, exact namespace counts, repeated facts, no default-stream fallback,
C#-derived/Go registration identity and unchanged positions after client restart.
Its C# request is hand-derived, not emitted by .NET. An unsupported kernel is a
failure, not a skip. See the [parity boundary](../parity.md#event-seeding).

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
Keep declarations stable across startups. The restart/reconnect integration gate
checks namespace-scoped event counts, payloads, type/generation, static tags and
unchanged sequence positions, including two equal entries in the global definition.
This verifies persisted seed identity; it does not promise kernel crash atomicity.

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
