---
title: Read-model reactors
description: React to best-effort model changes with Added, Modified and Removed conventions.
---

Use a read-model reactor for a side effect triggered by changes to a projection
or a locally reduced model. Use an [event reactor](reactors.md) when the work
needs durable event-observer delivery, failed partitions or replay. Read-model
changes have no durable cursor, checkpoint or retry guarantee.

## Register a convention-based reactor

Register the model and its contributing events, then explicitly admit the reactor
type before `NewClient`. The [watch example](../examples/watches/main.go) provides
a complete, runnable projection-backed workflow without a dependency container.
Its registration has this shape (`registry`, `model` and `names` are created there):

```go
err := chronicle.RegisterReadModelReactor[*AnnounceProduct](
    registry, model,
    func() *AnnounceProduct { return &AnnounceProduct{names: names} },
)
```

`NewClient` discovers exact exported names `Added`, `Modified` and `Removed`.
The model parameter is `M`, `*M`, `[]M` or `[]*M`. An optional leading
`context.Context` follows Go conventions. Remaining parameters are `events.Context`
or services resolved through the existing `chronicle.WithServices` seam.
Factories use the same service precedence and ownership rules as event reactors;
a nil factory requests service activation. No constructor runs during validation.

Prefer a pointer or collection for `Removed`: a window departure or deletion can
carry no model. It receives nil or an empty slice respectively; present values
become a one-element slice. A nonnullable value parameter cannot represent absence
and produces a reported error rather than a fabricated zero-valued model.

Handlers may return nothing, `error`, a registered event/effect, or `(effect, error)`.
Returned effects use the existing event-reactor pipeline, including event lists,
explicit routes, concurrency scopes and registered side-effect extensions.
Bare events target the changed key; provider interfaces can override it. Effects
run before cleanup and are never blindly retried after an ambiguous append.

## Scope and failure behavior

Each matching method or callback receives a fresh scope and artifact activation,
then its effects and cleanup run. This matches C#'s per-dispatch ownership, not the
event reactor's default per-batch scope. Go runs callbacks serially per registration
to preserve change order and bound concurrency; separate registrations can overlap.
No internal lock is held around application code. Callbacks, constructors, effects
and cleanup must honor cancellation.

Every matching callback runs even if another fails. Activation, invocation,
effect and cleanup failures are reported through
`reactors.WithReadModelErrorHandler(func(context.Context, error))`; the default
logs through `slog`. Dispatch failures do not retry or stop later changes.
Terminal watch failures are also reported and stop that registration for the
current generation. They do not block unrelated reads/appends after initial
readiness. A new client generation attaches new watches, with no claim that
intervening changes were recovered. Materialized differs and reducer first-seen
tracking start fresh on that generation.

The client owns these workers and joins them on close. Use
`store.UnregisterReadModelReactor(ctx, id)` to cancel and join one registration
across current and retired generations; removal survives reconnect. Never
synchronously unregister or close the client from that reactor's own callback.
An uncooperative callback can delay cleanup; `CloseContext` reports incomplete
cleanup rather than claiming it stopped.

`reactors.WithReadModelReactorID` overrides the default full Go type name.
`WithReadModelWatchOptions(readmodels.WithWatchBuffer(...))` changes the bounded
queue. Overload terminates that watch; it never silently drops a change or turns
best-effort work into a durable event observer.

## Explicit callbacks

`chronicle.RegisterReadModelReactorHandlers(registry, id, model, handlers, options...)`
admits a callback-only registration. Create each entry with
`reactors.ReadModelOn(readmodels.Added, callback)` (or `Modified`/`Removed`).
Callbacks use the same validated signatures, scopes and effects as convention
methods. `WithReadModelHandler` adds callbacks to a convention registration.
All matching callbacks run in registration order, each in its own scope. This is
also the Go alternative to C# overloads; Go cannot declare several methods with
the same name. Duplicate registration IDs fail rather than replacing an artifact.

For local reducers, successful observed batches emit `Modified`/`Removed`
notifications (not passive one-shot reads).
Read-model reactors infer `Added` on the first non-removal for a key and forget
that key on removal. These notifications are client-local, not a sink feed or an
acknowledged kernel write.

## Materialized reactors

Pass `reactors.Materialized(nil)` to select C#'s `[Materialized]` workflow. This
chooses the **materialized observe RPC**, not another sink. Nil selects skip 0 /
take 50; a `*readmodels.Window` configures membership.

The first snapshot produces `Added` for each keyed model. Subsequent snapshots
are compared by serialized model value: added/modified entries follow current
window order, then removals follow previous window order. The registered key
property takes precedence over `id`, `_id` and `Id`. Missing or duplicate keys
fail explicitly rather than C#'s silent omission/overwrite. Stored event-position
metadata outside the model does not manufacture a modification.

A removal has no value and means only that the model left this window. It may
still exist elsewhere in the sink. Causing-event positions, types and timestamps
are unavailable for these diffs; do not use them for deduplication or concurrency
protection. The kernel can coalesce intermediate snapshots.

See [watch lifecycle and windows](read-models/index.md#watch-changes) for readiness,
release, cancellation and buffering contracts, and [parity](parity.md#read-model-watches-and-reactions)
for behavior-by-behavior evidence and the remaining history surface.
