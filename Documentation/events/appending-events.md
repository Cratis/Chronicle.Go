---
title: Append events
description: Inspect append outcomes and select explicit, optimistic or protected-empty concurrency scopes.
---

Use `store.EventLog().Append(ctx, sourceID, event, options...)` to persist one registered fact. A sequence position is unsigned and sequence-wide, not a source revision; the first position is zero. `EventSequence(id)` returns another sequence handle and a construction error for a blank ID. The event log is the same sequence implementation.

## Inspect both errors and results

`Append` returns `(eventsequences.AppendResult, error)`. Always inspect the operation error before calling `result.Err()`:

- `Committed` plus a non-nil `Position` confirms persistence. Zero is a valid position.
- `Rejected` means constraint or concurrency violations prevented storage, even when accompanied by errors. It preserves every diagnostic and returns a nil operation error. `result.Err()` exposes `ConstraintError` and `ConcurrencyError` through `errors.As`, including both when present.
- `Unknown` is not success. Errors-only kernel failures may occur before or after a durable commit, so both the operation error and `result.Err()` expose `OutcomeUnknownError` through `errors.As`. Diagnostics remain in `result.Errors` and in the unwrapped error, inspectable with `errors.Is(err, eventsequences.AppendError(message))`. Transport or protocol failures also expose `OutcomeUnknownError`. **Do not blindly retry.**
- Local validation, unknown events and known pre-dispatch credential/closed-client failures do not claim a commit. Authorization/validation envelopes are known rejections; execution exceptions or missing/inconsistent response envelopes remain ambiguous.

C# exposes `IsSuccess` and `Errors` without a disposition. Go classifies errors-only failures as `Unknown` because the kernel cannot distinguish pre-commit from post-commit failures in its response.

A result includes correlation, whether concurrency checking actually ran, every constraint's name/type/message/details/type ID/position, all append error codes, and a client-independent observer completion target. Waiting for observers is not implemented yet; append success means persistence, not completed side effects.

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

For more than one event, use [atomic batches](batches.md). Use [ReadHistory](reading-events.md) to protect earlier loaded state. Transactions, append notifications, constraint declarations, enrichment hooks and field-derived routing/subjects are not yet implemented. Use only the supported options; unimplemented field tags fail explicitly. See [parity and limitations](../parity.md).
