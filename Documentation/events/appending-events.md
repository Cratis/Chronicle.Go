---
title: Append events
description: Inspect append outcomes and select explicit, optimistic or protected-empty concurrency scopes.
---

Use `store.EventLog().Append(ctx, sourceID, event, options...)` to persist one registered fact. A sequence position is unsigned and sequence-wide, not a source revision; the first position is zero. `EventSequence(id)` returns the cached handle for that store/namespace/sequence and a construction error for a blank ID. The event log is the same sequence implementation.

## Inspect both errors and results

`Append` returns `(eventsequences.AppendResult, error)`. Always inspect the operation error before calling `result.Err()`:

- `Committed` plus a non-nil `Position` confirms persistence. Zero is a valid position.
- `Rejected` means constraint or concurrency violations prevented storage, even when accompanied by errors. It preserves every diagnostic and returns a nil operation error. `result.Err()` exposes `ConstraintError` and `ConcurrencyError` through `errors.As`, including both when present.
- `Unknown` is not success. Errors-only kernel failures may occur before or after a durable commit, so both the operation error and `result.Err()` expose `OutcomeUnknownError` through `errors.As`. Diagnostics remain in `result.Errors` and in the unwrapped error, inspectable with `errors.Is(err, eventsequences.AppendError(message))`. Transport or protocol failures also expose `OutcomeUnknownError`. **Do not blindly retry.**
- Local validation, unknown events and known pre-dispatch credential/closed-client failures do not claim a commit. Authorization/validation envelopes are known rejections; execution exceptions or missing/inconsistent response envelopes remain ambiguous.

C# exposes `IsSuccess` and `Errors` without a disposition. Go classifies errors-only failures as `Unknown` because the kernel cannot distinguish pre-commit from post-commit failures in its response.

A result includes correlation, whether concurrency checking actually ran, every constraint's name/type/message/details/type ID/position, all append error codes, and a client-independent observer completion target. Waiting for observers is not implemented yet; append success means persistence, not completed side effects.

## Observe command-attributable append attempts

Use `unsubscribe := sequence.OnAppend(func(eventsequences.AppendNotification))`
to observe local append results; `defer unsubscribe()` releases the callback.
`EventStore` shares sequence handles and subscriptions per client, store, namespace
and sequence, like C#. This is **not** a durable event subscription, global
interception or an observer-completion signal. Other stores, namespaces, sequences,
processes and clients do not notify it. Low-level `eventsequences.New` is the escape
hatch for independent, handle-local subscriptions.

Given a sequence handle, assign the command a fresh origin and collect its
immediate attempts like this (imports: `sync` and `eventsequences`; `ctx` is the
command context):

```go
origin := eventsequences.NewOrigin()
ctx = eventsequences.WithOrigin(ctx, origin)
var mu sync.Mutex
var attempts []eventsequences.AppendNotification
unsubscribe := sequence.OnAppend(func(n eventsequences.AppendNotification) {
    if n.Origin != origin {
        return
    }
    mu.Lock()
    defer mu.Unlock()
    attempts = append(attempts, n)
})
defer unsubscribe()
```

Pass that context to the handlers' appends. Read `attempts` under `mu` after
command-owned appends have returned; disposal does not join callbacks.

Each notification contains:

- `CorrelationID`: the effective **request** correlation, including an explicit
  append override or generated ID. It remains available when the response is lost,
  but several executions can legitimately share it.
- `Origin`: the unit's identity for `Owner.Commit`, otherwise the origin installed
  in the append context. Zero means unattributed.
- `Operation`: store, namespace, sequence and distinct exact event types.
- `Events`: original input order, source, type and generation, normalized route,
  and a position only when committed. Zero is a valid position. No event payload
  or mutable caller-owned event value is retained.
- `Result`: the unchanged disposition and complete violations/error codes, as a
  `BatchResult` even for one event. Its correlation remains the kernel's response
  correlation; use `Origin` to distinguish executions sharing a correlation.
- `Err`: the original operation error. A nil error alone does not mean success;
  inspect `Result.Disposition` and `Result.Err()`.

`Append`, `AppendMany`, `AppendBatch`, their metadata wrappers, and
`AppendPreparedBatch` deliver once per nonempty request, including named-tag
variants, known rejections and unknown outcomes. Notifications start only after
local preparation succeeds and the request is handed to the RPC client. Local
validation/serialization/scope-resolution failures and known pre-dispatch
transport failures do not notify: callers must
still check returned errors. Eventless checks do not notify either.

[Unit-of-work](unit-of-work.md) staging does not notify. A nonempty commit uses
`AppendPreparedBatch` and notifies **before** the unit's completion callback;
rollback, empty completion and repeated commit do not emit accepted events. Never
infer a notification's disposition from `IsCompleted` or a callback having run.

### Distinguish executions sharing a correlation

Correlation groups related work; it does not uniquely identify an execution.
Arc.Go reactor batches can run several commands with one delivery correlation,
when each command needs to collect only its own append attempts.

`eventsequences.NewOrigin()` creates an opaque, comparable, nonzero token. Tokens
are never reused within a process and remain valid after the execution completes.
They are process-local: do not persist them, compare across restarts or use them
as authorization credentials. Allocation is concurrency-safe; exhausting all
uint64 identities panics rather than reusing one. There is no global subscription
or correlation-to-command registry.

`eventsequences.WithOrigin(ctx, origin)` installs a token without changing audit or
wire metadata. `OriginFrom(ctx)` returns zero if none is installed; installing
zero masks an inherited token. Immediate single/many/batch appends, metadata and
named-tag variants inherit the append context's origin. Prepared batches use the
**append-time** context, not the preparation context.

`transactions.Begin` assigns a fresh origin, exposed by `UnitOfWork.Origin()`.
`Owner.Commit` always notifies with that identity, ignoring origins in Begin,
Stage or Commit contexts. The identity stays available after completion; nil and
zero units return zero. Filter by `unit.Origin()` to track that unit's commit, or
by a separate context token to track immediate writes without collecting the
unit's commit. Do not filter by zero to claim ownership of unattributed work.

`AppendNotification` is an SDK-produced callback value; consumers should receive
it rather than construct positional literals. Existing correlation, results and
wire contracts remain unchanged.

### Callback ownership and disposal

Callbacks run synchronously in subscription order before the append returns,
without internal locks held. They may subscribe, unsubscribe or append again;
bound recursive appends yourself. A subscription added during delivery receives
future deliveries, not the current one. Concurrent appends may invoke the same
callback concurrently and have no cross-operation ordering. Protect shared state;
slow callbacks delay the append caller.

Each callback owns its notification's collections, maps and positions; changing
those cannot alter another callback or the append result. Error objects are
borrowed read-only. A panic becomes `AppendCallbackPanicError`, joined with any
append error, while other subscribers still receive the original result. A
committed result stays committed: notification failure does not authorize retry.

Unsubscribe is idempotent and safe during delivery, including from the callback
itself. It prevents new callback admissions but **does not wait** for already
admitted callbacks. Join command-owned append calls before disposing and releasing
command state. Keep callback state concurrency-safe while shared-handle appends
may still be running; the callback closure remains alive for admitted deliveries.

For Arc.Go-style command tracking, set `metadata.WithCorrelation` before invoking
handlers, install a fresh `WithOrigin` token, subscribe to each sequence on their
store/namespace, filter `n.Origin`, and
retain `Committed`, `Rejected` and `Unknown` separately. A later empty transaction
completion cannot erase an immediate unknown outcome. To observe only immediate
writes, unsubscribe before the transaction owner commits and inspect its retained
result separately, as C# Arc does. Subscribe to each explicitly used sequence;
there is no process-wide interception.

[ExampleSequence_OnAppend](../../eventsequences/example_notifications_test.go)
is an executable command-scoped example with a lost acknowledgment. Run it without
a kernel using `go test ./eventsequences -run '^ExampleSequence_OnAppend$'`.

## Choose a concurrency expectation

| Expectation | Behavior |
| --- | --- |
| Omitted scope | Resolve the current source/route tail, then compare during append; an empty tail is unchecked |
| `eventsequences.Resolve()` | Query the tail using the supplied scope's exact narrowing |
| `eventsequences.Exact(position)` | Reject a matching tail greater than `position`; lower or absent tails pass |
| `eventsequences.NoMatchingEvent()` | Protect the absence of matching history |
| `eventsequences.NoCheck()` | Explicitly disable checking; combining it with narrowing is rejected |

Default optimistic resolution detects a race between its tail query and append. It does **not** protect business state read earlier. Use `Exact(position)` from that read instead. Despite the name, this supplies the kernel's upper bound, not an equality condition; `Exact(10)` accepts a tail of `3` or no history. Use `NoMatchingEvent()` when absence itself must be protected.

This excerpt protects first append for one source. It assumes the registered `CustomerRegistered` and `store` from [getting started](../clients/go/getting-started.md):

```go
source := events.SourceID("customer-42")
scope := eventsequences.Scope{
    Expectation: eventsequences.NoMatchingEvent(),
    Filter: eventsequences.ScopeFilter{SourceID: &source},
}
result, err := store.EventLog().Append(ctx, source,
    CustomerRegistered{Name: "Ada"}, eventsequences.WithScope(scope))
```

Check `err` and `result.Err()` as above. A competing append is rejected rather than both writers winning; the real-kernel integration test exercises this contract.

`ScopeFilter` can narrow source type, stream type, stream ID and event types. Nil dimensions do not narrow. The kernel's `Default`/`All` sentinels remain wildcards; do not treat them as exact read filters. A single append cannot guard a different source: use an independent labeled scope in [AppendBatch](batches.md) to guard another source.

Reserved sequence sentinels are invalid exact positions. Protected absence sends both the unavailable number and its dedicated wire flag. If a kernel commits while reporting that a requested check did not run, Append returns the committed result **and** `ErrUnsupported`. This is not safe to retry and is not advertised as protected success.

## Resolve configured policy for selected dimensions

When a command chooses concurrency dimensions, call
`sequence.ResolveScope(ctx, filter)` for each target source separately. It uses
`WithDefaultConcurrencyStrategy` (or the default optimistic strategy), honors
`WithCheckFirstAppendIntoAScope`, and returns an owned, resolved `Scope`. Supply
normalized dimensions: nil does not narrow, and this method adds no append-route
defaults. Custom strategies receive that selected filter.

Pass the result to `WithScope`, `WithScopes` or `transactions.UnitOfWork.Stage`.
Neither staging nor dispatch rereads that resolved tail or invokes the strategy
again. With first-append protection enabled, an empty matching tail becomes
`NoMatchingEvent`; otherwise it stays unchecked but resolved, retaining its filter.
The method never appends or retries. It protects the interval after resolution,
not state read earlier; use `ReadHistory` for earlier loaded state.

`ExampleSequence_ResolveScope` is an executable example of resolving and staging
an independent check without appending.

## Routing and audit metadata

Use `WithRoute`, `WithOccurred`, `WithSubject`, `WithTags`, `WithNamedTags` and `WithCorrelation` for per-append options. Source type defaults to `Default`, stream type to `All`, stream ID to `Default`; the stream ID is not inferred from the source. Subject defaults to the source ID. Occurrence defaults to kernel append time; supplied times retain their offset and truncate sub-100ns precision on the wire. The pinned MongoDB kernel persists these times at millisecond precision.

`metadata.WithCorrelation`, `WithIdentity` and `WithCausation` capture immutable context metadata. Correlation is a UUID, also sent as `x-correlation-id` RPC metadata. Causation is an ordered chain; actor on-behalf-of chains keep the first occurrence of each subject. Authentication identifies the client, while `CausedBy` identifies the actor. Do not put credentials or sensitive payloads into causation properties.

Named tags retain exact name/value pairs. A name must be nonblank; an empty value is valid. Identical records are coalesced without flattening values to strings or losing distinct values under one name.

For more than one event, use [atomic batches](batches.md). Use [ReadHistory](reading-events.md) to protect earlier loaded state. Enrichment hooks and field-derived routing/subjects are not yet implemented; use explicit [units of work](unit-of-work.md), [constraint declarations](constraints.md) and the scoped append notifications above. Use only the supported options; unimplemented field tags fail explicitly. See [parity and limitations](../parity.md).
