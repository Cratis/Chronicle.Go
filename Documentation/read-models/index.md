---
title: Read models
description: Register model schemas and read typed or raw state without confusing absence with a zero value.
---

Use `chronicle.RegisterReadModel[T]` to declare a model before constructing the client. A model describes state produced by a projection or reducer; **registration alone does not create an instance**. This page covers catalog registration, one-shot reads, watches, materialized windows, projection sessions and release. [Projection authoring](../projections/index.md), [collections and history](collections-and-history.md), and [decision reads](decision-reads.md) have their own references.

## Registration reference

`RegisterReadModel[T](registry, options...)` returns `(readmodels.Model[T], error)`. `T` must be a named, non-pointer struct supported by the shared [serialization contract](../events/event-types.md). Unsupported shapes and conflicting protection tags fail before network I/O.

| Option | Default and contract |
| --- | --- |
| `WithIdentifier(id)` | Full Go import path plus `.` and the simple type name. Set an explicit C# fully qualified name to share a model across languages |
| `WithContainerName(name)` | Case-preserving English plural of the simple type name: `Person` → `People`, `OrderSummary` → `OrderSummaries`. Independent of the identifier |
| `WithDisplayName(name)` | Simple Go type name |
| `WithGeneration(generation)` | One; must be positive. This selects the current schema, not a migration chain |
| `WithSink(readmodels.Sink{...})` | `MongoDB`, all-zero configuration UUID. Supported types: `MongoDB`, `SQL`, `InMemory`, `NoSink`; configuration IDs use canonical UUID strings |
| `WithObserver(kind, id)` | `Projection`, empty producer ID. Associates a producer; does not register it |
| `WithEventSequence(sequence)` | `event-log`; used for immediate reads and session cleanup |
| `WithIndexes(paths...)` | None. Serialized dot paths can traverse nested objects and collection items |
| `WithPII(paths...)` | None. Marks properties with kernel PII metadata, including composite leaves and coarse collections |
| `WithProtection(declarations...)` | None. Type/property PII or distinct subject/namespace/global confidentiality classifications and providers |
| `WithSubjectProperty(path)` | No explicit subject property. Selects a top-level scalar property for release, falling back to the Go `ID` field; `subject` tags use the same resolver |

Use `chronicle:"index"` on a model field for the model-bound equivalent of
`WithIndexes`. Tag paths and explicit paths accumulate into the same metadata;
duplicates involving a tag fail with `DeclarationError` (explicit-only duplicates
retain `ErrInvalidConfiguration`). Nested objects and
collection items are traversed using serialization-plan names. Traversal stops
recursive cycles per path, so two properties of the same nested type both retain
their indexes. Index tags neither subscribe to events nor create a projection.

Scalar options are last-wins. Collections are copied; duplicate index/PII paths, unknown paths, zero generations and blank required names fail. Neither PII nor confidentiality can protect an `events.SourceID` value, including through type or nested metadata. No public API requires a UUID dependency or a concept/date wrapper.

`WithRegistry` freezes declarations at `NewClient` time. `WithRegistryForStore` replaces the entire catalog for that store. Later registry changes do not alter existing clients. Duplicate Go model types or identifiers fail atomically.

The store registers definitions after event types and before returning a ready handle. Reconnect replays them through the existing registration barrier. Definitions are store-wide; instance requests and sessions carry the selected namespace. Registration uses C#'s `Client` owner and `Code` source. Inspect `store.ReadModels().Catalog()` through `Descriptors`, `Lookup`, `LookupType` or `LookupIdentifier` without I/O.

## One-shot reads

Create a typed reader with `readmodels.For(store.ReadModels(), model)`. Its `Get(ctx, key)` returns `(readmodels.Instance[T], error)`:

- Check the error first, then `Exists`. Only use `Value` when `Exists` is true.
- `LastHandled` is a separate `*events.SequenceNumber`. Nil means unavailable; position zero is valid. A removed model can be absent and still have a last-handled position.
- A present empty object is a valid zero-valued model. JSON `null` is absence; malformed, empty or non-object documents fail with `chronicle.ErrProtocol`.
- Present models normalize absent/null slice fields to empty slices, including nested collections. Use a pointer-to-slice to retain nullable presence. Maps retain their own null semantics.

For raw reads, call `store.ReadModels().Get(ctx, model.Identifier(), key)`. It returns `Instance[json.RawMessage]`, preserves stored metadata and never manufactures an absent document. Keys are preserved verbatim; blank keys are invalid and the kernel's `*` replay wildcard is unsupported by this one-instance API.

Reads require a declaration in the service catalog. A typed reader also requires the exact declaration retained from registration, not a separately constructed equivalent. Unknown/foreign declarations return `ErrNotRegistered`. Materialized reads are eventually consistent; neither `LastHandled` nor a session is a concurrency-protection token.

## Watch changes

`reader.Watch(ctx, options...)` returns a `*readmodels.Subscription[readmodels.Change[T]]`
only after the kernel sends `Subscribed`. **Subscription readiness is not a current
model snapshot.** Establish the watch before producing events you need to observe.
A projection watch does not require a reducer.

Use `Recv()` for one change at a time, or range over `subscription.Values()`.
`Change.Type` is `Added`, `Modified` or `Removed`; `Key` is always available.
Only use `Value` when `HasValue` is true. A removal may carry the old model or no
value. `Context` preserves the available position, occurrence and correlation;
the event type and undisclosed audit fields remain unknown. Requests use the
model's configured event sequence, rather than C#'s hard-coded event log.

The raw equivalent is `store.ReadModels().Watch(ctx, model.Identifier())`.
Both APIs validate the returned namespace and normalize model IDs/collections as
one-shot reads do. Protected documents pass through the same fail-closed `Release`
path before delivery. Chronicle 19.29.4's string-PII handler treats already released
plaintext as a no-op; missing subjects, failed releases and invalid documents are
terminal, never ciphertext fallbacks.

### Lifetime, interruption and overload

- The opening context controls the entire subscription, not just startup. Always
  call `Close()` after early consumption; it cancels and joins the receive worker.
  `Values()` also closes on early break. One consumer may receive at a time.
- `Done()` signals worker completion. Drain `Recv()` to receive queued values
  followed by the terminal error, or inspect `Err()` after `Done()`.
- An interrupted stream ends with `ErrInterrupted`, retaining its transport
  status or EOF. Caller cancellation retains its context error. There is **no
  silent resubscribe, durable cursor or gap-free resume**. Re-watch and re-fetch;
  a separate refetch is not an atomic handoff with the change feed.
- Defaults are 64 queued messages and 8 MiB of serialized data. Override both
  with `readmodels.WithWatchBuffer(messages, bytes)`. Exceeding either limit,
  including one oversized message, ends the subscription with `ErrOverloaded`.
  Decoded values and gRPC transport buffers are additional memory. No change is
  silently discarded while reporting success.
- Watches are independent of hydration sessions: no session selector exists on
  the watch RPC. Closing a session does not close a watch, and closing a watch
  does not dehydrate a session. Neither supplies a protected decision token.

### Local reducer notifications

For a registered reducer, `Watch` attaches to successful folds delivered through
this client's reducer observation runtime instead. Passive one-shot reads do not
publish into this feed. Readiness is a local attachment, not a server
`Subscribed` signal. Values are released before delivery; changes are `Modified`
or `Removed`, with unknown event metadata. This does not observe other clients,
prove a sink write, or provide durable recovery. Local notifications and decoded
deliveries each have the configured bounded queue. Generation replacement ends
existing watches; retired folds cannot publish into a new generation's watches.

## Materialized windows

`reader.Materialized().GetInstances(ctx, window)` reads sink-backed instances.
`ObserveInstances(ctx, window, options...)` returns a subscription of **complete
replacement windows**, not changesets. The first `Recv()` yields the initial
snapshot. This RPC has no `Subscribed` marker: opening waits for the first
successfully decoded and released window instead. The kernel may coalesce
intermediate snapshots under load. Release and decoding preserve receive order.

Pass nil for the C# defaults, skip 0 / take 50, or pass
`&readmodels.Window{Skip: 5, Take: 10}`. Negative skip becomes zero. Take zero or
negative means empty except `readmodels.UnlimitedInstances` (-1). Aligned ranges
use a kernel page directly; non-aligned ranges fetch a covering page from the
start and slice locally, capped at `math.MaxInt32`. Large covering/unlimited
requests can be expensive. Passive models have no sink window and fail explicitly.

The raw APIs are `store.ReadModels().Materialized().GetInstances` and
`ObserveInstances`, with the model identifier before the window argument.
Subscriptions have the same cancellation, buffering and terminal-error contract
as `Watch`. To react to window membership changes, use a
[materialized read-model reactor](../read-model-reactors.md#materialized-reactors).
Leaving a window is not proof of document deletion.

## Hydration sessions

`reader.NewSession(key)` returns a lazy `*readmodels.Session[T]`. It requires a declared projection identifier. `session.Get(ctx)` hydrates and subsequently reads using one opaque session ID. A session is permanently bound to its model, key, store, namespace and event sequence. Reducer sessions fail with `ErrUnsupported` rather than pretending to dehydrate server state the kernel does not support.

Call `session.Close(cleanupCtx)` **before closing the client**, including after a failed or canceled read. The kernel may have hydrated state even when the response was lost. Cleanup uses the actual sequence, not an unconditional event-log request. Close is idempotent after success; failed cleanup returns its error and can be retried with a fresh, bounded context. Once cleanup starts, further reads return `ErrClosed`.

Sessions own no background goroutine. Reads and cleanup serialize with cancellation-aware admission. Cancellation of a waiting cleanup does not cancel another caller's read. Reconnect does not promise preservation or automatic reconstruction of hydrated state; protected decision sessions are not implemented.

## Release externally loaded documents

All model reads verify release before delivery, including one-shot reads, sessions, projection replay, watches and materialized windows. Kernel handlers pass already released values through. For ciphertext loaded directly from a sink, use `reader.Release(ctx, value)`, `reader.ReleaseMany(ctx, values)` or `store.ReadModels().Release(ctx, model.Identifier(), rawDocument)`.

Release uses the model schema and namespace. Raw documents preserve `__subject` and `__subjects` lineage; a stored `__subject` takes precedence, followed by the configured subject property and then the Go `ID` field's serialized name. Without protection metadata, typed values pass through and raw documents are copied without an RPC.

Unlike C#'s best-effort release, Go returns `ErrRelease` and no model when a present subject-protected value has no owner, the release RPC fails, the kernel reports a release error, or the response is malformed. Inspect `*readmodels.ReleaseError` with `errors.As`; context and unsupported-operation identities survive wrapping. Error text omits payloads and subjects.

Namespace/global confidentiality can release without a subject. See [compliance and confidentiality](../compliance.md) for field/type classifications, erasure, reauthorization and pinned-kernel limitations. Unsupported codecs and conflicting declarations fail rather than silently publishing plaintext. Kernel release remains authoritative for cryptographic correctness and erased-value behavior.

## Run the example

The [read-model example](../../examples/readmodels/main.go) creates a unique store, registers an inventory model and verifies that an uncreated item is absent. It requires Go 1.26 or newer and a running `cratis/chronicle:19.29.4-development` kernel. It uses development credentials and disables certificate validation; do not use that configuration in production.

```sh
CHRONICLE_INTEGRATION_CONNECTION_STRING=chronicle://localhost:35000 \
  GOWORK=off go run ./examples/readmodels
```

Expected output:

```text
Example.InventoryItem registered; item-1 exists: false
```

For watches, projection-backed callbacks and an initial materialized window,
run the [watch example](../../examples/watches/main.go):

```sh
CHRONICLE_INTEGRATION_CONNECTION_STRING=chronicle://localhost:35000 \
  GOWORK=off go run ./examples/watches
```

It prints `watch: product-1 = Coffee`, `reactor: Coffee`, then
`materialized window: 1 product`. It also uses development-only credentials/TLS
and retains a uniquely named store.

Each read-model example run retains its uniquely named store in your development kernel. It does not delete data or install a projection. See [parity and migration notes](../parity.md#read-model-migration) for the C# mapping and remaining boundaries.
