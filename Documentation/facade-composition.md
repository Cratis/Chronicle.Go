---
title: Prepare a client with a shared provider
description: Bind one borrowed client identity before preparing Chronicle definitions with Fundamentals.Go scopes.
---

Use captured preparation when one shared provider must contain the client identity
and supply services needed to prepare that same client's definitions. This
experimental v0.x API does not automatically bind stores or facades. For ordinary
clients, keep using `NewClient` or `NewClientContext`; they capture and prepare in
one call and return only a prepared client.

## Compose the provider before preparing definitions

1. Register events, models and definition factories in a Chronicle registry.
2. Call `chronicle.CaptureClient(options...)`. Set all client configuration here,
   including any append-origin resolver and `chronicle.WithLogger(logger)`.
3. Register `p.Client()` using `dependencyinjection.BindValue(&bindings, p.Client())`.
   This is an explicit **borrowed** singleton, not an owned constructor result.
4. Register application collaborators, then build the provider. Do not open a
   preparation scope from inside a provider factory.
5. Call `services.PrepareClient(ctx, p, provider)`. Await its result before creating
   consumers that read catalogs, connecting, or obtaining store handles.
6. Use the returned client normally. It is exactly the identity from `p.Client()`.

The [executable example](../services/example_preparation_test.go) runs this complete
sequence with a provider-backed seeder and an offline catalog read:

```sh
go test -run ExamplePrepareClient ./services
```

The example prints `true 0`: identity is unchanged and its selected catalog has no
events. Neither capture nor preparation performs SDK transport I/O. Configuration
option functions still execute during capture; arbitrary application callbacks
can perform effects that Chronicle cannot prevent or roll back.

An Arc SDK consumer that reads catalogs during construction must be created after
preparation. This slice verifies a local catalog consumer, not actual Arc SDK
adoption or a published cross-module pairing. Automatic client/store/facade binding
helpers remain outside this API.

## Preparation contract

`CaptureClient(options ...ClientOption) (*ClientPreparation, error)` applies and
validates options, captures every selected registry once, and initializes the
client's lifecycle. It does not invoke schema, factory, service, or selector
preparation callbacks. Registry collections are detached; immutable declaration
plans retain their codecs, classifications and original model identities. Borrowed
callbacks and their hidden closure state are not cloned.

`p.Client() *Client` always returns the same identity. Do not copy a `Client` value,
let a provider default-construct it, or transfer its cleanup ownership. During
preparation a constructor can borrow this exact `*Client` through an explicit
binding, but operational use returns `ErrNotPrepared`. Visible own-store/sequence
dependencies and SDK facade artifact results are rejected before activation.
Chronicle checks the identity of clients it resolves directly. The provider and
application remain responsible for provider-internal graphs and borrowed ownership.

`p.Prepare(ctx, scopes reactors.ScopeFactory) (*Client, error)` is the
container-independent core API. `services.PrepareClient` adapts a Fundamentals.Go
scope factory and preserves its optional service catalog. Both borrow the factory
for preparation and runtime observers; neither closes the provider.

- Nil scopes selects captured `WithServices`, or the container-free default.
- Before admission, a nil context, typed-nil factory, or explicit factory combined
  with captured `WithServices` is invalid and does not consume the attempt.
- An already canceled context consumes the admitted attempt and retains failure.
- Only one attempt is admitted. Concurrent and reentrant calls return
  `ErrPreparationInProgress` immediately, including while cleanup runs.
- Completed calls ignore their arguments and return the retained result without
  callbacks or factory reevaluation. Retry requires a new capture.
- All selected metadata and visible definition/seeder constructor plans are
  preflighted before the first temporary scope. Each distinct captured registry
  compiles once. Every snapshot is published together, only after cleanup succeeds.

Before successful preparation, `Connect`, `Ready`, `EventStore`, `EventStores`,
`Catalogs`, and `Artifacts` fail before options, selectors or transport effects.
`Ready` does not retry a preparation-state error. A callback may handle a denial,
and an external readiness probe does not invalidate preparation. Actual errors,
cancellation, closure or cleanup failure prevent publication.

State diagnostics contain only fixed operation/category text. Preparation errors
use the existing payload-free `PreparationError`; panic payloads are discarded.
See [definition factory errors and ownership](definition-factories.md).

## Use one application-owned logger

Pass `chronicle.WithLogger(logger)` to capture your application’s `*slog.Logger`
for SDK lifecycle, preparation and observer diagnostics. The last client logger
option wins; a final nil (including a typed nil) fails configuration validation.
Without the option, `CaptureClient` captures `slog.Default()` at that call, not
later at `Prepare` or reconnect. `NewClient` and `NewClientContext` capture and
prepare immediately. Options and selected registries are frozen at capture;
changing the global default or reusing an option slice afterward cannot reroute
that client.

`reactors.WithLogger`, `reducers.WithLogger` and
`reactors.WithReadModelLogger` override the client fallback for their artifact,
including subscription recovery. An explicit choice remains explicit even when
its pointer equals the default logger. Each distinct registry compiles once per
client; store bindings and reconnect reuse those immutable plans.

Loggers and handlers are borrowed, never closed by Chronicle. Chronicle does not
call `slog.SetDefault`. Your handler must support concurrent synchronous calls,
honor cancellation, and avoid reentering client lifecycle methods or blocking
shutdown. Chronicle contains handler panics without logging their values or
recursively calling the same handler; it cannot make an arbitrary blocking
handler harmless. Handler failures do not change dispatch, commit or acknowledgment
outcomes.

SDK-owned records carry fixed operation, stage and bounded category strings.
They omit payloads, credentials, tokens, principals, raw metadata, source IDs,
partition keys, correlation IDs and arbitrary error text. Classification inspects
only exact trusted identities/types; it does not invoke application error
formatting, traversal or gRPC status hooks. Wrapped or unknown errors use the
coarse `failure` category. Even logical artifact names are omitted: choose
non-sensitive names if you add them in your own logging. Fields already attached
to your logger, handler context inspection and application-written logs are your
responsibility. This contract does not redact returned error graphs, explicit
`WithReadModelErrorHandler` callbacks or kernel exception-message wire fields.

The option configures SDK diagnostics, not constructor dependency injection.
If a factory needs `*slog.Logger`, capture it in a closure or bind it explicitly
in your provider; the container-free resolver does not fabricate a zero logger.
The [compiled worker example](../example_logging_test.go) uses a constructor
closure and one logger without a container. Run it with:

```sh
go test -run ExampleWithLogger .
```

This is Go-specific `slog` routing, not full C# `ILogger`/`ILoggerFactory` DI parity
or a generic logging adapter. Logging framework bridges remain separate recipes;
they add no dependencies to the root SDK.

## Close the client, join preparation, then close the provider

Preparation is synchronous caller-owned work, not client-owned background work.
Its context combines caller cancellation with the client lifetime. Callbacks and
closers execute outside SDK locks and may call `Client.Close()` without waiting
for themselves.

`Close`, `CloseContext`, and `Shutdown` cancel preparation but **do not join its
callbacks or cleanup**. A deadline cannot forcibly terminate a non-cooperative
callback. An outstanding preparation call remains your responsibility even after
client closure completes.

On normal shutdown or startup failure:

1. Stop and join application work.
2. Close Chronicle.
3. Join any outstanding `Prepare` or `PrepareClient` call, including its cleanup.
4. Close the provider.

A client closed before preparation admits no callbacks and cannot publish catalogs.
After a successful preparation, closure is independent: repeated `Prepare` still
returns that same, now-closed identity. Operational calls return `ErrClosed`, while
`Catalogs` and `Artifacts` remain available as historical frozen configuration.
