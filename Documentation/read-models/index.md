---
title: Read models
description: Register model schemas and read typed or raw state without confusing absence with a zero value.
---

Use `chronicle.RegisterReadModel[T]` to declare a model before constructing the client. A model describes state produced by a projection or reducer; **registration alone does not create an instance**. This page covers catalog registration, one-shot reads, projection sessions and release. Projection authoring, watches, history and protected decision reads are separate capabilities.

## Registration reference

`RegisterReadModel[T](registry, options...)` returns `(readmodels.Model[T], error)`. `T` must be a named, non-pointer struct supported by the shared [serialization contract](../events/event-types.md). Unsupported shapes and protection tags fail before network I/O.

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
| `WithPII(paths...)` | None. Marks scalar string properties with kernel PII metadata; other protection shapes remain unsupported |
| `WithSubjectProperty(path)` | No explicit subject property. Selects a top-level string property for release, falling back to the Go `ID` field |

Scalar options are last-wins. Collections are copied; duplicate index/PII paths, unknown paths, zero generations and blank required names fail. PII cannot protect the model key or subject. No public API requires a UUID dependency or a concept/date wrapper.

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

## Hydration sessions

`reader.NewSession(key)` returns a lazy `*readmodels.Session[T]`. It requires a declared projection identifier. `session.Get(ctx)` hydrates and subsequently reads using one opaque session ID. A session is permanently bound to its model, key, store, namespace and event sequence. Reducer sessions fail with `ErrUnsupported` rather than pretending to dehydrate server state the kernel does not support.

Call `session.Close(cleanupCtx)` **before closing the client**, including after a failed or canceled read. The kernel may have hydrated state even when the response was lost. Cleanup uses the actual sequence, not an unconditional event-log request. Close is idempotent after success; failed cleanup returns its error and can be retried with a fresh, bounded context. Once cleanup starts, further reads return `ErrClosed`.

Sessions own no background goroutine. Reads and cleanup serialize with cancellation-aware admission. Cancellation of a waiting cleanup does not cancel another caller's read. Reconnect does not promise preservation or automatic reconstruction of hydrated state; protected decision sessions are not implemented.

## Release externally loaded documents

The kernel releases protected values on its own `GetInstanceByKey` path. Do not decrypt those results a second time. For ciphertext loaded directly from a sink, use `reader.Release(ctx, value)` or `store.ReadModels().Release(ctx, model.Identifier(), rawDocument)`.

Release uses the model schema and namespace. Raw documents preserve `__subject` and `__subjects` lineage; a stored `__subject` takes precedence, followed by the configured subject property and then the Go `ID` field's serialized name. Without protection metadata, typed values pass through and raw documents are copied without an RPC.

Unlike C#'s best-effort release, Go returns `ErrRelease` and no model when a protected document has no subject, the release RPC fails, the kernel reports a release error, or the response is malformed. Inspect `*readmodels.ReleaseError` with `errors.As`; context and unsupported-operation identities survive wrapping. Error text omits payloads and subjects.

This is a bounded PII-string release surface, not the full compliance SDK. Type/container-wide PII, encrypted classifications, custom codecs, key erasure and reauthorization are not implemented. Unsupported `chronicle` field tags fail rather than silently publishing plaintext. Kernel release remains authoritative for cryptographic correctness and erased-value behavior.

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

Each run retains its uniquely named store in your development kernel. It does not delete data or install a projection. See [parity and migration notes](../parity.md#read-model-migration) for the C# mapping and remaining boundaries.
