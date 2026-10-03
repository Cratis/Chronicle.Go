---
title: Prepare a client with a shared provider
description: Bind one borrowed client identity before preparing Chronicle definitions with Fundamentals.Go scopes.
---

Use captured preparation when one shared provider must contain the client identity
and supply services needed to prepare that same client's definitions. This
experimental v0.x workflow uses the optional `services` adapter to bind borrowed
client, store and facade instances. Root Chronicle packages remain independent of
dependency injection. For ordinary clients, keep using `NewClient` or
`NewClientContext`; they capture and prepare in one call.

## Compose the provider before preparing definitions

1. Register events, models and definition factories in a Chronicle registry.
2. Call `chronicle.CaptureClient(options...)`. Set all client configuration here,
   including any append-origin resolver and `chronicle.WithLogger(logger)`.
3. Call `services.BindClient(&bindings, p.Client())`. This registers that exact
   identity as a **borrowed** singleton, not an owned constructor result.
   `dependencyinjection.BindValue` remains a valid explicit alternative.
4. Call `services.BindEventStore(&bindings, selector)`, then the facade helpers
   needed by application collaborators. Register those collaborators and build
   the provider. Do not open a preparation scope inside a provider factory.
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
adoption or a published cross-module pairing. Actual Arc consumer adoption remains
tracked in [Arc.Go #29](https://github.com/Cratis/Arc.Go/issues/29).

## Select and borrow facades per operation

After preparation, open a provider scope using trusted host metadata. Resolve the
store or facade through that scope. The first store resolution performs ordinary
SDK connection and registration I/O; provider construction does not.

| Helper | Exact key | Lifetime and ownership |
| --- | --- | --- |
| `BindClient` | `*chronicle.Client` | Borrowed Singleton |
| `BindEventStore` | `*chronicle.EventStore` | Borrowed Scoped |
| `BindEventLog` | `*eventsequences.Sequence` | Borrowed Scoped |
| `BindEventTypes` | `*events.Catalog` | Borrowed Scoped |
| `BindReadModels` | `*readmodels.Service` | Borrowed Scoped |
| `BindCompliance` | `*compliance.Manager` | Borrowed Scoped |

Bind the store once and then any number of different facade helpers. Every facade
forwards the same instance exposed by that store. Across scopes, `Client.EventStore`
reuses its existing cache keyed by **store and namespace**; there is no adapter-wide
cache or cross-provider cache.

The [source example](../services/example_bindings_test.go) shows both provider and
plain store access, application-owned `SourceStoreMetadata`, append-origin options
captured before preparation, and a metadata/principal guard. `ExampleBindEventStore`
compiles as part of the unit suite; direct execution needs a development kernel.
`TestFacadeSourceExampleUsesRealStoreAPI` runs its composition function against an
in-process gRPC fixture, not a real kernel.

A consumer interface must resolve its concrete key and return that **same borrowed
instance**, with the same lifetime, rather than call another constructor. Multiple
targets use application wrapper types with existing `BindBorrowed` factories (see
`AuditLog` in the example). Fundamentals v0.1 has no named/keyed registration here;
these helpers do not introduce one or register a provider for callbacks to retain.

### Selection and failure lifetime

`StoreSelector` has signature
`func(context.Context) (chronicle.StoreName, chronicle.Namespace, error)`. Both
coordinates must be nonblank. Selection is host metadata, **not authorization**;
never derive tenancy from untrusted message payloads. The host installs trusted
metadata and owns access checks. Fundamentals' `container.WithContextGuard` can
reject changed coordinates, principal or metadata presence even for cached values;
the helper neither installs that guard nor authenticates the caller.

Selection runs synchronously, once per scope, on the first resolving caller's
context, preserving its values and deadline. Supply a bounded operation context,
honor cancellation in the selector, and join resolution before closing the scope.
There is no background invocation. Other callers can cancel their own waits
without canceling the owner. An owner cancellation freezes failure even if its
callback returns successful coordinates. A canceled caller rejected by the provider
before selection starts has not consumed the selection attempt.

A private, resource-free scoped cell is cached **before** selection runs. It keeps
only copied coordinates or a safe error, never a context or resolver. Store
registration failure can retry in the same scope with those coordinates; a selector
failure, panic or owner cancellation requires a **new scope** to select again.
`SelectorError` formats only fixed text. Its cause follows the adapter's sanitized
diagnostic grammar; direct context errors remain inspectable, blank names expose
`ErrInvalidConfiguration`, and recovered panics expose `ErrSelectorPanicked`, never
their payload. Sensitive coordinate values are not included in diagnostics.

Selectors must only read metadata: **do not resolve services or call the SDK from a
selector**, including via a retained/global scope. Declared dependency cycles and
singleton-to-scoped dependencies fail during Fundamentals provider build. Hidden
resolution cannot be detected by the adapter: a reentrant store lookup can wait on
the provider's already-in-flight store factory before reaching a selection-cell
check. Context recursion markers cannot fix that boundary. Such callbacks are
unsupported; no goroutine-identity heuristic or hidden resolver is provided.

An unprepared client remains bindable. Store resolution returns `ErrNotPrepared`
without SDK transport I/O and does not poison later preparation. The application
metadata selector may run **before** that root guard; this is not a guarantee of
callback-free resolution. The adapter never calls `Ready` as a startup probe.
Closing the client before first facade resolution returns `ErrClosed`. Already
cached facades remain borrowed handles whose operations obey SDK closure rules.

### Registration is not a transaction

Nil clients, selectors and registrars are rejected. With an optional `Catalog`,
each helper preflights all keys it adds (including the private cell), duplicates
and missing direct dependencies before registering anything. Register the client
before the store, then the facades. Without `Catalog`, graph/missing-key validation
belongs to provider build or resolution. These are exact-key checks, not proof of
arbitrary provider factories' identity or ownership.

Helpers return `Register` failures unchanged and never replace existing bindings.
An arbitrary `Registrar` offers no rollback: if cell registration succeeds but
store registration fails, the cell is left registered. Discard that partial
registrar; do not continue composing or retry the batch. The cell owns no external
resources. Previously successful helper calls also remain registered if a later
helper fails. Registration is single-owner; preflight is not concurrent mutation
protection. Interoperability evidence here uses Fundamentals v0.1's actual container,
not a claim of native support for other containers.

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

Before successful preparation, root `Connect`, `Ready`, `EventStore`, `EventStores`,
`Catalogs`, and `Artifacts` fail before their options, selectors or transport effects.
The optional adapter's metadata selector is outside that root guard, as above.
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
reusing an option slice afterward cannot replace that client's logger or handler
identity. Their underlying state and output destinations remain application-owned,
not snapshotted.

The pristine `slog.Default()` handler writes through `log.Default()`'s current
writer. Changing that writer affects an already captured logger. In particular,
`slog.SetDefault(customLogger)` installs a standard-log bridge to the custom
handler, so diagnostics through the captured pristine logger can reach that new
handler (with the original record rendered as message text). By contrast, a
custom logger selected before capture retains its handler identity across later
`slog.SetDefault` calls. For strict destination isolation, pass `WithLogger` an
explicit logger whose handler and writer remain stable; Chronicle does not
replace the pristine default with an SDK-owned handler.

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
handler harmless. A logging handler called on SDK-owned work must not call
`Client.Close`: joining that work from its own callback can deadlock. These are
cooperative callback limits, not a guarantee of harmless caller blocking. Handler
panics do not change dispatch, commit or acknowledgment outcomes.

SDK-owned records carry fixed operation, stage and bounded category strings.
They omit payloads, credentials, tokens, principals, raw metadata, source IDs,
partition keys, correlation IDs and arbitrary error text. Classification inspects
only exact trusted identities/types; it does not invoke application error
formatting, traversal or gRPC status hooks. Wrapped or unknown errors use the
coarse `failure` category. Even logical artifact names are omitted: choose
non-sensitive names if you add them in your own logging. Fields already attached
to your logger, attributes added or changed by a borrowed handler, mutable writer
state, handler context inspection and application-written logs are your
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
