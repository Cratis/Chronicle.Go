---
title: Stage an ordered unit of work
description: Share immutable event staging with nested participants while retaining one completion owner.
---

Use `transactions.Begin` when several participants must contribute to one atomic append. The unit fixes one store, namespace and sequence, and preserves global enrollment order: A1, B1, A2 stays A1, B1, A2. This API is experimental in the v0.x SDK. Use [decision reads](../read-models/decision-reads.md) to enroll admitted projection reads into an owned commit.

## Participant and owner

`Begin(ctx, sequence)` returns `(*UnitOfWork, *Owner, error)` without I/O. Obtain the sequence from an initialized event store. The unit borrows that handle; it does not own or close the client.

Keep the owner in the outer execution scope. Pass the participant directly or use `WithUnitOfWork(ctx, unit)` and `FromContext(ctx)` for nested calls. These accessors never install the owner, open a new transaction, or replace a completed participant with a successor. `Begin` always creates a new unit; nested code joins through `FromContext`, not another `Begin`.

| Operation | Contract |
| --- | --- |
| `unit.Stage(ctx, entries, scopes...)` | Serialize and snapshot one complete enrollment without I/O; a failed call enrolls nothing |
| `owner.Commit(ctx)` | Freeze staging and attempt at most one ordered atomic batch; return `(eventsequences.BatchResult, error)` |
| `owner.Rollback()` | Discard open work; a deferred call after terminal completion does nothing |
| `unit.GetEvents()` | Return defensive JSON snapshots in staging order, not live Go values |
| `unit.State()`, `IsCompleted()`, `IsSuccess()` | Distinguish lifecycle completion from successful commitment |
| `unit.Result()` | Return a defensive copy of the final batch diagnostics and the operation error |
| `unit.OnCompleted(callback)` | Replace the completion callback; invoke it synchronously, outside locks, after commit success, failure or rollback |
| `unit.TryGetLastCommittedEventSequenceNumber()` | Return the last confirmed position and a presence flag; zero is a valid position |

The diagnostic getters `GetConstraintViolations`, `GetConcurrencyViolations` and `GetAppendErrors` return copies. Operation errors, including transport failure, remain available through `Result`.

Register `OnCompleted` before terminal completion; late registration returns `ErrCompleted`. Its callback may inspect results or call deferred rollback without deadlocking. It must return promptly. A callback panic propagates, but cannot change the already-recorded final disposition.

## What staging captures

`Stage` accepts the same [entries and labeled scopes](batches.md) as `AppendBatch`. It captures serialized event content, routing, static and dynamic tags, named tags, subject, occurrence time, causation and scope filters before returning. Do not mutate input data concurrently with `Stage`; changes after it returns cannot affect the commit.

Correlation and actor are fixed at `Begin`. An absent correlation is generated once. A stage with no correlation inherits the unit's; a different nonzero correlation or actor is rejected. Derive nested contexts from the begin context so identity remains intact. Each entry keeps its own stage context's causation chain, extended by `Entry.Causation`. If that chain is empty, the kernel may supply request causation. Commit's context controls cancellation and transport metadata, not the staged payload's actor, correlation or audit chain.

Each stage is contiguous. Concurrent calls are ordered by successful enrollment, not goroutine start time. A call racing completion either enrolls entirely before the freeze or fails with `ErrCompleting`/`ErrCompleted`. Serialization runs without the unit's state lock.

Explicit scopes replace implicit first-entry-route defaults. Repeating an identical explicit scope across stages is allowed; event-type filters compare as sets. Conflicting explicit scopes reject the whole stage. Duplicate labels within a single stage still fail. Unlike an omitted scope, an explicit `Resolve` filter is a real enrolled check and cannot later be replaced by a different expectation.

Use `Sequence.ReadHistory` and stage its `history.Scope()` when events depend on loaded state. The expectation comes from that history, never a newer tail. Omitted and explicit `Resolve` scopes query their matching tails only at commit; an initially empty resolution is unchecked. See [history concurrency](reading-events.md).

## Completion and failure

The lifecycle is `Open → Completing → Committed | Rejected | OutcomeUnknown`, or `Open → RolledBack`. Every commit attempt is terminal, including cancellation before dispatch. Staging after completion fails; committing again returns `ErrCompleted` and the retained result without another append. Rollback during an in-flight commit returns `ErrCompleting`.

Check the operation error and then `result.Err()`. Interpret the disposition even when an error accompanies the result:

- **Committed:** the kernel confirmed persistence. A missing requested concurrency check can still return this disposition plus `ErrUnsupported`; do not retry. A completely empty unit completes successfully without an RPC and has no persisted positions.
- **Rejected:** local failure before the write, a rejected command envelope, or atomic constraint/concurrency rejection establishes that no events were committed. Domain violations remain structured diagnostics; they normally have a nil operation error.
- **OutcomeUnknown:** transport loss, malformed acknowledgement, or a kernel response containing only append errors cannot establish whether events persisted. The kernel can catch exceptions after persistence. The owner will not retry; reconcile before deciding what to do next.

`IsCompleted` is not evidence of persistence. Rollback only discards pending work: it cannot undo a committed or ambiguous batch. An early aggregate commit completes the shared owner, not only that aggregate. Later staging fails, and later command failure cannot undo it. Arc integrations must retain the owner outside ordinary handlers and must not silently open a successor unit.

An empty unit is different from a scope-only unit. Staging explicit scopes without events commits through the eventless validation path; it requires an effective protected check. Older-kernel refusal remains `ErrUnsupported`, not a successful no-op. Enrolling a decision token through `unit.Enroll` also counts as work with no staged events; its protected path rejects unchecked mixed scopes.

## Run the example

The [unit-of-work program](../../examples/unitofwork/main.go) stages A1, joins a nested participant for B1, stages A2, commits once, and verifies the persisted order. With a disposable development kernel running as described in [getting started](../clients/go/getting-started.md), run from the repository root:

```sh
go run ./examples/unitofwork
```

Expected output:

```text
Committed A1, B1, A2 at [0 1 2]
```

Set `CHRONICLE_INTEGRATION_CONNECTION_STRING` to select another development endpoint. Each run creates a uniquely named store. The example accepts a self-signed development certificate; production clients must use validating TLS and credentials. Reset only the disposable kernel's storage when you no longer need the sample data.

For C# migration differences, see the [parity ledger](../parity.md).
