---
title: Revise, redact, and close event history
description: Explicit history operations, audit metadata, asynchronous outcomes, and irreversible effects.
---

Use revision to correct recorded content, redaction to remove it, and stream
completion to refuse future appends to a specific stream. These are explicit
administrative operations, not ordinary domain compensation. This surface remains
experimental in the v0.x SDK.

**Authorize and approve every operation in your application.** A selected store,
namespace, source, or `metadata.WithIdentity` is not authorization. Check retention
and legal-hold requirements before dispatch. Do not put personal data, credentials,
or the content being removed in reasons, actor names, or causation properties.

## Operations and scope

Obtain a `*eventsequences.Sequence` from `store.EventLog()` or
`store.EventSequence(id)`. Every call is restricted to that handle's store,
namespace and sequence. Calls honor context cancellation, use the normal client
registration/connection barriers, and never retry or stage in a unit of work.

| Method | Target and result |
| --- | --- |
| `Redact(ctx, position, reason)` | One sequence-wide position, including zero. Nonblank `events.RedactionReason` required; the three highest positions (`Unavailable`, C# `Max` and `BeforeFirst`) are invalid. Nil error means the asynchronous request was accepted. |
| `RedactForEventSource(ctx, source, reason, typeIDs...)` | All matching events for one nonblank source, across **all source types and streams**. No type IDs means **all types**. IDs must be registered; each selects all generations. Nil error means accepted, not applied. |
| `Revise(ctx, position, replacement)` | One existing event at an actual position, including zero. A registered value or nonnil pointer supplies the type ID, generation and shared serialization plan. The kernel protects classified content with the original event's subject; see [protected revisions](#protected-revisions) for the minimum kernel and refusals. Nil error means accepted, not applied. |
| `CompleteStream(ctx, streamType, streamID)` | Permanently closes that explicit pair across **all event sources** in the sequence. Both arguments must be nonblank. Returns the **sequence-wide** tail at closure, or `events.Unavailable` for an empty sequence. |

Source redaction is not a frozen snapshot: the kernel selects matching events
when it executes the request. It does not prevent later appends. To target one
known occurrence, use its sequence position; an event source ID is not a stream ID.

## Audit a request

Actor and ordered causation links come from `metadata.WithIdentity` and
`metadata.WithCausation`. Without them, the SDK sends Chronicle's NotSet actor and
an empty chain, just as other explicit Go writes do. Capture the actual operator
and approval reference before calling; do not manufacture an authenticated actor
from untrusted input.

This excerpt assumes an authorized `ctx` and a connected `sequence`:

```go
ctx = metadata.WithIdentity(ctx, identities.Identity{Subject: "approved-operator"})
ctx = metadata.WithCausation(ctx, metadata.Causation{
    Occurred: time.Now().UTC(),
    Type: "history-correction",
    Properties: map[string]string{"ticket": "case-40"},
})
if err := sequence.Redact(ctx, 12, "approved removal"); err != nil {
    return err
}
```

The [executable example](../../eventsequences/example_history_test.go) also shows
revision, filtered source redaction and completion. Run it without a kernel or a
container:

```sh
go test ./eventsequences -run ExampleSequence_Redact -v
```

Its offline transport only acknowledges requests; it does not simulate storage
mutation. Real-kernel behavior belongs to the integration suite.

`Revise` has no dedicated reason field: record why in a non-sensitive causation
link. Revision and redaction contracts carry actor/causation, but no caller
correlation field. The kernel generates a fresh correlation for the system
request; a context correlation is not an idempotency key or an application receipt.
`CompleteStream` has no actor, reason, or causation fields on its wire contract.
Keep an external authorization/audit record for stream closure.

## Know what changed

**Revision retains history.** The kernel requires the replacement's type ID to
match the original event; it uses the supplied registered generation. It adds a
revision rather than appending an event or changing the original occurrence.
Reads expose current `Content`, `OriginalContent`, ordered `Revisions` and
`GenerationalContent`. Use `events.Decode[T]` with the selected-store catalog to
request a registered generation. Registering a new generation and migration alone
is [event evolution](evolution.md), not a correction to one occurrence.

**Redaction removes content irreversibly from the targeted history.** It replaces
the stored payload with `EventRedacted`, removes revisions and content hashes, and
retains the position and routing. It is not a database-row deletion. The outer
`Appended.Context` describes the redaction; the marker retains the original type
ID, occurrence, correlation and identity IDs. `appended.Redaction()` decodes the
marker without registration. `appended.Decode(catalog)` and
`events.Decode[events.EventRedacted](catalog, appended)` recognize it too.
`marker.OriginalType(catalog)` resolves a known current Go type or returns `any`
for an unknown ID, preserving `OriginalEventType` either way. Marker correlation
and identity IDs use `uuid.UUID`, so explicit observer registration also has a
supported schema codec.

Redaction is **not** a purge of backups, database oplogs, prior exports, external
side effects, or copies in another store's inbox. It also does not remove prior
system revision requests. Treat reasons and all remaining audit metadata as
retained data. Do not expect a local read, cache, or replica to forget a value it
already received.

## Acceptance is not application or replay completion

The kernel acknowledges `Redact`, `RedactForEventSource` and `Revise` after
appending system requests. A kernel reactor applies them later. A missing event,
type mismatch, unavailable generation or storage failure can therefore occur
**after a nil error**. The pinned contract exposes no mutation receipt/status RPC
that confirms the final application outcome
([Chronicle#4526](https://github.com/Cratis/Chronicle/issues/4526)).

Read-after-mutation checks must wait for the actual marker or revision under a
bounded deadline. Do not equate an unchanged value with a rejection or resubmit
because a read timed out. An append tail and observer completion target do not
fence revisions of old positions. Durable mutation/recovery barriers remain
[upstream work](https://github.com/Cratis/Chronicle/issues/3920).

The kernel replays affected partitions for replayable observers subscribed to the
original event types. Projections rebuild from corrected history. Reactors can
receive revised content during replay and can handle markers by explicitly
registering `events.EventRedacted` alongside their original event types. Existing
`OnceOnly`/replay policies still apply. This does not undo a previously sent email,
HTTP call or other external effect. Design those handlers for replay.

These methods **never emit local `OnAppend` notifications**, including rejected
or unknown outcomes. C# `AppendOperations` observes append calls, not history
mutation requests, even though the kernel internally appends system events.

## Errors and retry discipline

- Invalid targets/reasons return `ErrInvalidConfiguration` before dispatch.
  Unknown replacement types or source-filter IDs return `ErrNotRegistered`;
  replacement serialization failures also occur before dispatch, as do the
  [protected revision](#protected-revisions) refusals, which preserve
  `errors.Is(err, chronicle.ErrUnsupported)`. These are known pre-dispatch
  rejections, not `MutationOutcomeUnknownError`.
- Explicit authorization/validation envelope refusals remain inspectable as
  `*chronicle.EnvelopeError` with `errors.As`. They are not reported as accepted
  operations.
- Transport failures, exceptions after dispatch, and missing/inconsistent
  responses return `*eventsequences.MutationOutcomeUnknownError`. Its `Unwrap`
  preserves diagnostics, status and local error identities. Cancellation after
  dispatch does not undo the server operation. Reconcile; **never blindly retry**.
- Revision is not idempotent: repetition can add another revision. Point redaction
  of an already-redacted event preserves its first marker on the pinned kernel,
  but repeating the request can still create audit events. Repeating source
  redaction can reach new events. Neither is a safe automatic retry protocol.
- `CompleteStream` is synchronous. `errors.Is(err,
  eventsequences.StreamAlreadyCompleted)` means the pair remains closed;
  `eventsequences.DefaultStreamCannotBeCompleted` protects `All`/`Default`.
  Later appends to a closed pair are rejected with `StreamClosed`. There is no
  reopen operation or public stream-status RPC in the pinned contract. Returned
  `Unavailable` means an empty sequence; returned C# `Max` or `BeforeFirst`
  means `MutationOutcomeUnknownError` wrapping `ErrProtocol`, since the stream
  may already have closed. Do not use
  another destructive completion call as a status query.

## Compliance and compensation are separate

Use [cryptographic erasure](../compliance.md) for subject-key destruction. It
leaves event identity/history in place while making protected PII unavailable;
redaction removes all payload content for the selected events and does not erase
subject keys. Neither automatically compensates a business operation.

### Protected revisions

Like `Append`, `Revise` serializes the replacement as plaintext and leaves
protection to the kernel. From Chronicle 19.32.2
([Chronicle#4525](https://github.com/Cratis/Chronicle/issues/4525)) the kernel
applies the **replacement generation's** PII and subject, namespace and global
encryption metadata to the stored revision **and** to the `EventRevised` system
request before either is persisted. It protects them with the **original
event's subject**, or its event source when the original had no subject, so a
replacement cannot move protected data to another subject. Erasing that subject
shreds the PII the replacement generation classifies, in the revision and the
system request, as well as in the original content.

`TestKernelProtectedRevisionIsProtectedWithOriginalSubject` exercises this
against the pinned kernel: it revises an event whose subject differs from its
event source and carries PII plus namespace and global encryption, checks that
the revision releases, that neither the stored event nor the system request
holds plaintext (by scanning the kernel's MongoDB storage), and that erasure
removes the PII while leaving the encrypted values readable.

The SDK refuses two revisions with `ErrUnsupported` before serializing or
dispatching anything:

- A protected replacement when the kernel that would receive it is older than
  19.32.2 or its version was not verified (`WithSkipCompatibilityCheck`). The
  check is repeated on the connection that sends the revision, so a reconnect to
  an older kernel while providers or enrichers run also refuses it, without
  sending anything. Those kernels store revised content and the system request
  unprotected.
- On every kernel, an unclassified replacement whose type ID another registered
  generation classifies. The kernel protects only with the replacement
  generation's schema, so that plaintext could never be erased. Revise with the
  classified generation instead. C# dispatches this revision; Go refuses it
  deliberately.

Upgrading the kernel does not repair plaintext that earlier kernels stored.

Compensating events describe business corrections. Compensation/tombstone
metadata alone does not reverse history, redact data, or complete a stream.
For C# composed append operations, use an ordered `[]eventsequences.Entry` with
`AppendBatch`, per-entry causation/routes and `WithScopes`. The Go substitution
retains caller order rather than grouping repeated sources. History mutations
are deliberately not part of that atomic append batch.
