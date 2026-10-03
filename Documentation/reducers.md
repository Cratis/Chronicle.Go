---
title: Fold a read model in Go
description: Use plain callbacks or discovered reducer methods, nullable state, passive reads and optional service scopes.
---

Use a reducer when the next state needs control flow that neither model-bound nor
fluent projections can express. For field mappings, counters and collection
updates, start with a projection. This experimental reducer API supports active
materialization and passive reads; [the parity map](parity.md#reducers) lists the
remaining boundaries.

## Plain Go first

The [complete example](../examples/reducers/main.go) folds adjustments into a
balance. Recovering a negative balance credits half a positive adjustment, so a
simple projection counter cannot express the rule. No container or DI import is
required.

With a development kernel on localhost:35000:

```sh
go run ./examples/reducers
```

It prints `Balance: -6`. The example uses development TLS/credentials and creates
a new namespace in `plain-go-reducers` each run, leaving its events there.
`CHRONICLE_CONNECTION_STRING` overrides the local endpoint. Do not use its
connection settings in production.

This excerpt assumes the example's registered events, model and `fold` function:

```go
err = chronicle.RegisterReducerHandlers(registry, model, "account-balance",
    []reducers.Handler{
        reducers.On(fold),
        reducers.On(func(context.Context, AccountDeleted, *Balance, events.Context) (*Balance, error) {
            return nil, nil // Delete, not "ignore".
        }),
    }, reducers.Passive(), reducers.WithVersion("1"))
```

Check the error before calling `NewClient`. `reducers.On[E, M]` accepts
`func(context.Context, E, *M, events.Context) (*M, error)`. A nil current state is
absent; a non-nil zero-valued model is present. Return nil to delete, or return
current unchanged to ignore an event. An error discards the operation's state and
stops later events. Captured collaborators remain yours to close; callbacks can
run concurrently for different reads, stores or connection generations.

## Convention-based reducers

Register a reducer type and exactly one read model; Chronicle discovers its
exported methods when `NewClient` freezes the registry. Both authoring paths use
one fold plan. The [compiled convention example](../examples/reducers/convention.go)
uses the same events, model and fold function as the plain-Go example:

```go
type BalanceReducer struct{}

// Method names are descriptive; the first event parameter selects dispatch.
func (*BalanceReducer) Apply(ctx context.Context, event AmountChanged, current *Balance, ec events.Context) (*Balance, error) {
    return fold(ctx, event, current, ec)
}

func (*BalanceReducer) Delete(AccountDeleted, *Balance) *Balance { return nil }

func registerConvention(registry *chronicle.Registry, model readmodels.Model[Balance]) error {
    return chronicle.RegisterReducer[*BalanceReducer](registry, model,
        func() *BalanceReducer { return &BalanceReducer{} },
        reducers.WithID("account-balance"), reducers.WithVersion("1"))
}
```

Register the events and model first, then call `registerConvention` instead of
`registerPlain`. Unlike the passive callback example, this registration is active:
`readmodels.For(store.ReadModels(), model).Get(ctx, key)` reads eventually
consistent kernel-materialized state.

### Fold signatures and validation

After an optional leading `context.Context`, methods take:

1. A registered event `E`, `*E`, or nonempty interface implemented by registered events.
2. The associated current model `*M` (recommended), or `M`.
3. Optionally `events.Context`, containing the persisted event metadata.

Results are `M` or `*M`, optionally followed by `error`. There are no arbitrary
service parameters: use constructor injection. Prefer nullable `*M`, matching
C#. Go's value-current alternative supplies zero `M` for absence and cannot
distinguish an absent model from a present zero model.

The richest signature wins, followed by ordinal method name, with shadowing
diagnostics. Go can discover only exported methods. Duplicate explicit bindings,
invalid signatures, clearly fold-shaped unknown events, foreign model handles,
and conflicting projection/reducer producers fail at `NewClient` with
`*reducers.DeclarationError`. Constructors do not run during validation. A reducer
cannot own multiple models, and two reducers cannot own the same model.

Default identity is the full import path plus reducer type name, without a pointer
marker. Use `WithID` to preserve identity across renames or share a C# identity.
Without source metadata, the default is `event-log`. `events.WithSourceStore`
metadata selects the local log or an external inbox and provisions the shared
subscription. `WithEventSequence` or `WithEventLog` selects explicitly and
suppresses that inference. See [external integrations](integrations/index.md).

### Fingerprints, activity and filters

`WithVersion("1")` participates in a stable SHA-256 fingerprint alongside the
canonical fold shape, model identity, event generations and observer settings.
Equivalent explicit and discovered signatures produce the same default hash.
**Bump the version when method bodies, helpers or captured rules change.** Go
cannot hash executable IL like C#: the deterministic default detects shape
changes, not implementation-only changes. Generated source versions remain future work.

`reducers.Passive()` or `readmodels.Passive()` on a reducer's registered model
selects on-demand local folding and no sink. An unseeded/deleted model returns
`Exists=false`. Each `Reader.Get` reads source/type history and folds it in one
scope, then releases supported protected values before returning. It is not a
snapshot or concurrency token. Like C#, passive source/type reads do not apply
the active observer's metadata filters. `WithActive(false)` alone only stops
materialization; it does not turn reads into passive folds.

`WithEventTagFilter` accepts any matching appended tag; `WithEventSourceType` and
`WithEventStreamType` filter appended metadata. `WithTags` labels the reducer and
does not filter events. Filters apply to active kernel delivery. Scalar options
are last-wins, handler options accumulate, and slices are copied.

## Optional dependency injection

Plain constructors come first. If you already use a Fundamentals.Go scope factory,
pass `services.WithServices(provider)` to `NewClient` alongside the registry.
`services` is an optional import; the core SDK does not import a container or
`dependencyinjection`.

A constructor may take an optional context followed by dependencies or
`reducers.Scope`, and return `R` or `(R, error)`. A provider-advertised artifact
wins over its constructor; nil requests service/default activation. Optional
`Catalog` support validates dependencies at startup; without it, failed resolution
is reported during activation. Close the Chronicle client before its borrowed
provider. See [service ownership](reactors.md#dependency-injection-is-opt-in).

Each reduce operation gets one scope and artifact. Kernel-driven operations use
`identities.System()`; passive local reads retain the caller's identity during
construction and folds, matching C#'s direct invoker path.
`reducers.BatchFromContext` exposes store/namespace/sequence/reducer coordinates
before construction; event context is a method argument, not first-event state
captured by the constructor. Scope-resolved artifacts belong to the provider;
constructor results belong to Chronicle. Cleanup runs once, before acknowledgement.
Cleanup failure fails the operation instead of C#'s log-and-swallow behavior.

## Replay lifecycle callbacks

Implement `reducers.ReplayNotifier` for reducer-wide replay, or
`reducers.PartitionReplayNotifier` for partition replay. The exported method names
are exactly `BeginReplay`, `EndReplay`, `BeginReplayPartition` and
`EndReplayPartition`. Each interface requires its begin/end pair. Methods accept
`context.Context`, followed by `events.SourceID` for partition notifications, and
return `error`. Invalid signatures fail with `*reducers.DeclarationError` at
`NewClient`; similarly named prefixes have no lifecycle meaning. A valid ordinary
fold named `BeginReplay` still dispatches by event signature, not by its name.

For explicit folds, add callbacks with `reducers.WithReplayCallbacks`. Nil fields
are optional. This option also works with a registered artifact, but a callback
cannot replace an artifact method for the same notification. For example, with
your registered model and fold:

```go
err = chronicle.RegisterReducerHandlers(registry, model, "account-balance",
    []reducers.Handler{reducers.On(fold)},
    reducers.WithReplayCallbacks(reducers.ReplayCallbacks{
        BeginReplay: func(ctx context.Context) error { return ctx.Err() },
        EndReplay: func(ctx context.Context) error { return ctx.Err() },
        BeginReplayPartition: func(ctx context.Context, key events.SourceID) error {
            // key is the opaque kernel partition, not decoded event content.
            return ctx.Err()
        },
        EndReplayPartition: func(ctx context.Context, key events.SourceID) error {
            return ctx.Err()
        },
    }), reducers.WithVersion("1"))
```

Check the registration error. These callbacks show the accepted shapes; put your
own replay work inside them. `ExampleRegisterReducerHandlers_replay` compiles and
validates this authoring path without connecting to a kernel.

Each notification activates a fresh operation scope and artifact, independently
of normal folds, and finishes cleanup before the next stream message. Explicit
callbacks get their own scope too; captured collaborators remain caller-owned.
Constructors can resolve services, but notification and fold methods cannot take
arbitrary DI parameters. Scope-owned artifacts remain the provider's responsibility.

Like C# `Reducers.HandleReplayNotification`, notifications preserve incoming
identity, correlation and causation: they do **not** install the ordinary fold's
System identity or fabricate event metadata. `BatchFromContext` still supplies
reducer/store/namespace/sequence coordinates. Each callback receives a
stream-generation-bound context canceled after operation cleanup. Partition keys
are passed unchanged. Duplicate notifications are dispatched again; this is not
an exactly-once or durable replay-completion contract.

There is no notification result acknowledgement. Callback errors, panics,
activation failures and cleanup failures terminate only that reducer's stream,
which reconnects through the existing observer supervisor. Unlike C#'s
log-and-ignore activation failure, Go fails closed so a missed start cannot be
silently followed by continued processing. Unknown future notification states
also fail before activation rather than folding or reporting successful completion.
Bump `WithVersion` when lifecycle implementation changes, as for folds.

## Failure and shutdown

Folds execute in event order. Failure returns no partial state and reports the
last successful sequence position; cleanup failure resets it to unavailable.
The kernel owns partition recovery. Generation-specific payloads are selected
when supplied, otherwise C#'s current-content fallback applies. Historical codec
registration and migration authoring are not implemented.

Registration sends the frozen plan after event/model registration. It is not a
kernel acceptance or caught-up acknowledgement. Stream errors and normal EOF
resubscribe only that reducer after two seconds; C# reconnects on errors but has
no normal-completion handler. No reducer retry replays an application-side effect.

`store.UnregisterReducer(ctx, id)` cancels and joins local streams, including
retired generations, and retains removal across reconnect. It does not delete
kernel state or disable passive reads. Never call it synchronously from that
reducer's fold or lifecycle callback. `Client.Close` joins all workers; do not
call it synchronously from its own callback. A callback can call
`CloseContext(ctx)` with its operation context: cancellation releases that wait,
which returns an incomplete-cleanup error; an external later `Close` joins cleanup.
`CloseContext` bounds the wait, not a callback that ignores cancellation. Retired callbacks can overlap a new
generation until they honor cancellation, as with reactors.

Snapshots and bounded historical folds remain in #33; these replay callbacks do
not complete all of #31. Local watch notifications use successful folds, not
lifecycle callbacks. Reducer sessions remain unsupported.
