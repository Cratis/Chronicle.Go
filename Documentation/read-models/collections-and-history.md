---
title: Read-model collections and history
description: Read all model instances or correlation-grouped projection snapshots with explicit event-count and fidelity limits.
---

Use `reader.GetAll(ctx, count)` for a complete collection and
`reader.GetSnapshots(ctx, key)` for the states a projection passed through.
Both use a registered `readmodels.Model[T]` and its frozen serialization plan.
For paged sink queries or live updates, use [materialized windows and watches](index.md).

## Collections

```go
reader := readmodels.For(store.ReadModels(), model)

// Default retrieval: materialized state, or an on-demand passive model.
current, err := reader.GetAll(ctx, nil)
if err != nil {
    return err
}
for _, instance := range current.Instances {
    fmt.Println(instance.Value)
}

// Replay/fold the first 100 matching events across all sources.
count := events.Count(100)
historical, err := reader.GetAll(ctx, &count)
if err != nil {
    return err
}
fmt.Println(historical.ProcessedEventsCount)
```

This excerpt assumes a connected `store`, its registered `model`, a caller `ctx`,
and an enclosing function returning `error`.

The raw equivalent is
`store.ReadModels().GetAll(ctx, model.Identifier(), count)`.
Both return `Collection[T]` (raw: `Collection[json.RawMessage]`):

- `Instances` is an owned, non-nil slice on success. Every entry is present.
  Deleted partitions are omitted; a present zero-valued model remains.
- `ProcessedEventsCount` is the actual reported or locally folded count.
  Materialized retrieval reports zero, even with nonempty results.
- Each instance's `LastHandled` comes from reported progress, never `count - 1`.
  Missing/unavailable progress is nil; reserved positions are protocol errors.

### Count and routing

`events.Count` is a distinct domain from `events.SequenceNumber`.

| Count | Projection | Reducer |
| --- | --- | --- |
| `nil` | Materialized sink, or replay if passive | Materialized sink, or local fold if passive |
| `0` | Empty, no RPC | Empty, no RPC or reducer activation |
| `1..math.MaxInt32` | Kernel replay | Local fold, including active reducers |
| `events.UnlimitedCount` | Materialized sink, or replay if passive | Local fold, including active reducers |
| Other values | `ErrInvalidConfiguration` | `ErrInvalidConfiguration` |

Unlimited is `math.MaxUint64`, not zero. Keep it explicit when you want an active
reducer folded locally instead of reading its sink. Finite counts are rejected
above the limit, not clamped as in the C# local-fold implementation.

Local folds sort matching history, select the first N events **globally**, then
fold each source in first-seen order. Sparse sequence positions do not change
that bound. Source/type filtering follows C# historical reads; active observer
metadata filters do not narrow the query. Generation-aware decoding, caller
identity, scoped activation and cleanup use the production reducer plan. Query
folds emit no local change notifications or observer acknowledgments.

The history RPC is unpaged and has no count field. A local count bounds folding,
not the amount fetched or retained in memory. A missing local reducer plan fails
with `ErrUnsupported`; the SDK does not delegate the fold to a connected client.

## Projection snapshots

`reader.GetSnapshots(ctx, key)` returns `[]readmodels.Snapshot[T]`.
The raw equivalent is
`store.ReadModels().GetSnapshots(ctx, model.Identifier(), key)`.
Keys must be nonblank and cannot be `*`.

Each snapshot contains:

- `Instance T`: state after applying that correlation group.
- `Events []events.Appended`: raw, server-released contributing events with their
  type/generation, source, stream, position, occurrence, correlation, causation,
  actor chain, tags, named tags, hash, observation state and compliance subject.
  Store, namespace and sequence are the actual request coordinates.
- `Occurred`: the first contribution's occurrence, not transaction completion.
- `CorrelationID`: the group's correlation, not the response envelope correlation.

The kernel groups correlations **globally**. A/B/A becomes an A group containing
the first and third events, followed by B. It is not three contiguous snapshots
or a transaction timeline. Returned group order is preserved.

Use `events.Decode[E](store.EventTypes(), snapshot.Events[i])` to decode a known
contribution. Unknown Go event types remain raw. This RPC carries no event ID,
original content, revisions or alternate-generation payloads; those fields stay
absent. Do not assume it offers the representations available from sequence reads.

`Snapshot.Instance` deliberately has no `Exists` or `LastHandled`. Kernel 19.29.4
serializes a removed root as `{}`, which is indistinguishable from present empty
state. Empty history is a non-nil empty slice, not fabricated presence evidence.

## Supported replay shapes

Collections and snapshots are partial kernel replay capabilities:

- Reducer snapshots return `ErrUnsupported` before I/O. Kernel 19.29.4 returns an
  empty list for every non-projection; Go refuses to present that as absence.
- Projection collection replay/history requires a locally known producer with
  empty initial state and source-ID root keys. Nonempty defaults, custom keys,
  joins, joined removal, children and nested projections are refused. The SDK
  neither evaluates a local projection nor overlays defaults onto replay results.
- Catalog-only remote projections remain readable from their materialized sink,
  but replay/history fails without producer fidelity evidence. Low-level adapter
  constructors can supply `WithProjectionReplayValidator`; the adapter owns that
  evidence, not a heuristic based on the model schema.

## Protection, failure and lifetime

The pinned kernel owns release for materialized collections and projection
collection replay. Snapshot history releases contributing events **before**
projection. Local reducers fold already released sequence history into plaintext.
These routes validate documents/protected model shapes and decode them without a
second compliance RPC. Store services also validate known protected contribution
generations through their event codecs; low-level adapters supply
`WithSnapshotEventCatalog` for that check. Validation checks the declared shape,
not a heuristic that guesses ciphertext from string contents. Calling `Release` again on plaintext is not safe for every
provider. Use `Service.Release` for unreleased documents fetched directly from a
sink; it retains schema/subject-lineage grouping and fail-closed behavior.

All authorization/validation/exception envelopes are checked. A late RPC, schema,
codec, reducer, cleanup or cancellation error discards the complete result,
including progress and contribution metadata. Read-error messages omit payloads;
underlying causes remain inspectable with `errors.Is`/`errors.As` and can themselves
contain sensitive diagnostics. Application codecs execute outside locks and
counted transport leases, so they can close their client without self-deadlock.

These reads allocate no hydration sessions. Counts and snapshot metadata prove
neither observer readiness nor catch-up, durable replay completion, authorization
of a later write, or a [protected decision](decision-reads.md).
