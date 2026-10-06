---
title: External integrations
description: Declare cross-store subscriptions, outgoing webhooks and external HTTP or database services.
---

Use store subscriptions to consume another store's outbox, webhooks to declare
outgoing event delivery, and external services to describe kernel-owned HTTP or
database endpoints. None requires a dependency-injection container or installs an
HTTP server, database driver or background integration worker in your Go process.

These definitions are **store-wide**, not namespace-specific. A namespace handle
selects the same definitions as other handles for that store. Subscription
forwarding connects corresponding namespaces in the source and target stores.
Use separate stores when definitions must differ; namespace selection is not
authorization.

## Consume another store's events

Declare the origin when registering a consumed event:

```go
placed, err := chronicle.RegisterEvent[OrderPlaced](registry,
    events.WithID("OrderPlaced"), events.WithSourceStore("orders"))
```

This excerpt assumes an existing registry and an `OrderPlaced` struct. Handle
`err` before registering observers. Reactors, reducers and projections that
reference this event infer `inbox-orders` when registered in another store and
`event-log` when registered in `orders` itself. Unspecified origins do not narrow
the source. Multiple distinct named origins fail unless an explicit observer
sequence bypasses inference.

Registration unions event IDs from all external observers into one subscription
per source store, identified by that store's name. The order is:

1. Ensure the store and namespace; register event types and read models.
2. Register constraints, projections, reactors and reducers.
3. Provision external subscriptions.
4. Start read-model reactors, then seed events.

A failed subscription fails readiness and blocks seeding. Successful definitions
are shared across namespace handles and re-registered on reconnect. Reactor and
reducer protocols acknowledge the registration **send**, not kernel acceptance.

`reactors.WithEventLog()` and `reducers.WithEventLog()` explicitly suppress
inference and provisioning. `WithEventSequence` has the same effect for any
explicit reactor/reducer sequence. Observer-level `WithSourceStore` overrides the
events' origins, selects the named inbox even in that store, and cannot be mixed
with an explicit sequence. Projections follow C#: an explicitly selected
`inbox-<store>` still provisions; an explicit non-inbox sequence does not.

For explicit management, on an already registered target `store`:

```go
err := store.Subscriptions().Subscribe(ctx, "orders", "orders",
    placed.Descriptor().Ref().ID)
```

Check the result before continuing. `GetAll(ctx)` lists definitions;
`Unsubscribe(ctx, id)` removes one without deleting previously forwarded events.
An automatic subscription will be restored on a future registration generation
while its observers remain declared. Removal is asynchronously materialized by
the kernel; it is not a synchronous read-after-write barrier.

No event IDs means **all currently registered event IDs**, not a wildcard for
future types. IDs are deduplicated. Like C#, subscription filters use generation
one; an observer still requests its own declared generation. Source and target
must differ. Definitions do not move events appended to the ordinary event log:
the producer must publish into `source.EventSequence(events.Outbox)`.

## Declare an outgoing webhook

`store.Webhooks().Register(ctx, id, targetURL, options...)` submits a webhook.
`GetAll(ctx)` lists detached definitions; `Remove(ctx, id)` removes one.
`webhooks.Define` prepares an immutable snapshot for `RegisterDefinition`.

| Option | Default and behavior |
| --- | --- |
| `WithEventTypes(refs...)` | All current catalog types if omitted; exact generations retained; repeated filters deduplicated |
| `WithEventSequence(id)` | `event-log` |
| `WithReplayable(bool)` | `true` |
| `WithActive(bool)` | `true`; see the pinned-kernel limitation below |
| `WithHeader(key, value)` | Replaces the same exact key |
| `WithBasicAuth`, `WithBearerToken`, `WithOAuth` | No authorization; last authorization option wins |

The C# public Register builder supports Basic and Bearer. Go `WithOAuth` is a
converter/contract-backed convenience, not C# Register-factory parity. The
[actual-package authentication observations](authentication-evidence.md) compare
None/Basic/Bearer requests without claiming kernel authentication behavior.

Targets must be absolute HTTP(S) URLs without userinfo. Put credentials in the
authorization options, not the URL. Returned definitions expose `Identifier`,
`EventSequence`, `EventTypes`, `TargetURL`, `Headers`, `IsActive` and `IsReplayable`.
Authorization is intentionally **not reconstructed** on read-back: the kernel
never returns the credentials. Do not reuse read-back as a credential-preserving
update template.

The SDK sends explicit false fields to preserve C# protobuf defaults. In
19.32.3-development, MongoDB's webhook conversion still loses `IsActive=false`
on read-back, including C#-equivalent requests
([Chronicle#4394](https://github.com/Cratis/Chronicle/issues/4394)). Do not rely on
inactive webhook behavior on that kernel. Definition management is tested; HTTP
delivery/authentication and webhook updates are not end-to-end verified here.
Authorization events can also fail to append on this kernel when their internal
event schema is absent; command acceptance alone is not authentication evidence.

## Declare external services

`store.ExternalServices().Register(ctx, name, options...)` uses `name` for both
the persisted ID and display name. `externalservices.Define` plus
`RegisterDefinition` separates preparation from submission.

- `HTTP(url)` selects an HTTP endpoint. `WithHeader`, `WithBasicAuth`,
  `WithBearerToken` and `WithOAuth` configure it.
- `MSSQL(Database{...})` and `PostgreSQL(Database{...})` select database endpoints.
  `Database` carries `Host`, `Port`, `Name`, `Username`, `Password` and copied
  `Options`. Port zero leaves the provider default; otherwise use 1–65535.
- `WithOption(key, value)` adds a database provider setting.

Only the final selected endpoint kind is serialized. HTTP authentication does
not configure database credentials. A successful registration does not test
connectivity or install a Go database driver. C# `IExternalServices` exposes
registration only; administrative list/remove contracts are not wrapped here.

## Errors and sensitive values

Every operation accepts a context. Services borrow the store's connection and
registration barrier; they do not retry dispatched explicit writes. A transport
failure can leave an unknown write outcome. Inspect gRPC statuses and
`chronicle.EnvelopeError`; authorization, validation and execution failures are
not converted to success.

Definitions and database configuration redact `fmt` output, including `%#v`.
`KernelDefinition()` explicitly returns an owned protobuf snapshot **containing
secrets**. URLs, headers, provider options and kernel diagnostics can also be
sensitive. Do not log these exports. No secret configuration is logged by these
packages.

## Examples and capture declarations

Run the offline, container-free authoring example:

```sh
go run ./examples/integrations
```

It prints inferred `inbox-orders` selection and a capture declaration; it makes
no connection and activates nothing. The [example source](https://github.com/Cratis/Chronicle.Go/blob/develop/examples/integrations/main.go)
shows the complete imports and error handling.

For external polling definitions, continue with [Capture declarations](captures.md).
See the [per-behavior parity map](../parity.md#external-integrations) for exact
runtime boundaries.
