---
title: Read events and protect loaded history
description: Read ordered raw events with explicit tail absence and an expectation derived from loaded history.
---

Use `ReadHistory` when a subsequent write depends on source state. It returns the events you actually loaded and the matching concurrency expectation together. Use the other reads for browsing, imports or presence checks. These APIs remain experimental in the v0.x SDK.

## Read API reference

| Method | Return and boundary |
| --- | --- |
| `ReadSource(ctx, source, SourceFilter)` | Complete matching `[]events.Appended`, ordered by sequence position |
| `ReadFrom(ctx, from, FromFilter)` | Matching events **at or after** `from`, ordered by position |
| `ReadHistory(ctx, source, SourceFilter)` | `History` containing ordered events, normalized filter and expectation |
| `Tail(ctx, TailFilter)` | `(position, exists, error)`; empty history is `(0, false, nil)`, not a fabricated first event |
| `Next(ctx)` | Tail plus one, or zero for an empty sequence; not a reserved position |
| `HasEvents(ctx, source)` | Whether the source has any events in this sequence and namespace |

All methods accept caller cancellation and inspect kernel response envelopes. Invalid coordinates, filters and reserved positions fail rather than sending a misleading query. An invalid event or envelope fails the entire read; no partially trusted history is returned. Reads are finite unpaged RPCs, not live subscriptions or streaming iterators.

`SourceFilter` supports source type, stream type, stream ID and event types. `FromFilter` supports only source ID and event types because the from-position RPC has no route fields. `TailFilter` has the same dimensions as `ScopeFilter`.

Empty dimensions and the kernel's `Default` source type, `All` stream type and `Default` stream ID do **not** narrow. Read normalization trims dimensions, removes these wildcards, sorts/deduplicates type IDs and canonicalizes their generation to one: these queries and their concurrency checks match IDs across generations, not exact generation numbers. A source read still requires a nonblank source. An empty type set means all types.

## Save against the history you loaded

`History.Events` owns the loaded events. `History.Filter` records the normalized source/type/route filter. `History.Expectation` is:

- `NoMatchingEvent()` if the complete matching read returned no events;
- `Exact(lastLoadedPosition)` otherwise, including position zero.

`history.Scope()` returns a defensive copy of that filter and expectation. Pass it to `WithScope` for a single-source append, or enroll it with a matching `LabeledScope` in `WithScopes` for an atomic batch. The [batches example](batches.md#run-the-example) demonstrates this workflow.

The SDK never queries a newer tail after loading and substitutes it as your state's version. If another writer appends matching events between the read and your append, the old expectation rejects the write. A known rejection allows you to read again and reconsider the decision; an unknown write outcome does **not** permit a blind retry.

`Exact` is the kernel's upper-bound condition, not equality. This primitive detects a newer matching append; it does not promise a multi-query snapshot or protection against revisions/redaction of already-loaded events. Aggregate modeling and request-scoped units of work belong in downstream layers.

## Decode and retain event metadata

`events.Appended.Content` is owned `json.RawMessage`. Decode it with `encoding/json` into the Go shape for `Context.EventType`; reads never guess a registered generation or discard an unknown event type. `OriginalContent`, ordered `Revisions` and `GenerationalContent` retain the other representations returned by the kernel. Absent original content is nil.

`Context` includes store/namespace/sequence, source and stream routing, type/generation/tombstone, position, occurrence, subject, actor chain, correlation, causation, ordinary/named tags, hash and observation state. Result maps and slices are not shared with the client.

The SDK sends occurrence at .NET-compatible 100 ns precision and preserves the timestamps it receives. The pinned **19.29.4-development MongoDB kernel** stores occurrence and causation dates at millisecond precision, so finer input precision does not survive a persisted read. This is a server storage limitation, not a promise that the Go client can restore lost digits. See [parity and limitations](../parity.md).
