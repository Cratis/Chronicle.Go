---
title: Observer and job administration
description: Inspect failures, request replay and distinguish accepted work from actual processing completion.
---

Use `store.Observers()` to inspect observer state and failures, and `store.Jobs()`
to inspect the work scheduled by an explicit replay. Both services retain the
store handle's namespace and connection lifecycle. They do not start recovery
workers or retry administration mutations.

This reference assumes a connected `*chronicle.EventStore`; see
[Get started](clients/go/getting-started.md) for registration and connection setup.
The [executable examples](https://github.com/Cratis/Chronicle.Go/blob/main/observation/example_test.go)
use an offline transport to demonstrate result handling without a server.

## Read observer state and failures

| Call | Result |
| --- | --- |
| `Observers().List(ctx)` | Immutable observer snapshots in the current namespace |
| `Observers().Get(ctx, id, sequence)` | One snapshot; `nil, nil` for gRPC NotFound |
| `Observers().FailedPartitions(ctx, id)` | Failure records with every attempt; empty ID selects all observers |
| `sequence.TailForObserver(ctx, eventTypes)` | Matching **source** tail and an explicit presence boolean; not an observer checkpoint |

Use `Information.EventTypes()` or a reactor plan's declared types for the tail
filter. Empty filters fail instead of reading the unfiltered sequence tail.
`Information.LastHandled()`, `Next()` and `Tail()` preserve reserved sequence
sentinels, including `events.Unavailable`. Position zero is valid.

`SubscriptionKnown()` is true only for `Get`: the kernel does not query live
subscriptions when building `List`. A listed `IsSubscribed() == false` means
**unavailable**, not confirmed disconnected. Neither value proves producer
readiness. `IsReplayable()` preserves protobuf-net's default-true policy and
explicit false for reactor-wide `OnceOnly` declarations.

Failure snapshots preserve partition identity, failure kind, timestamp and offset,
messages, stack trace, resolution and quarantine flags. Clearing a quarantine is
not the same as resolving a failed attempt. Diagnostic text may contain sensitive
application data; do not log whole snapshots indiscriminately.

Snapshots and their nested collections are immutable: accessors return value
copies or detached slices. Holding a snapshot does not refresh it.

## Replay, recovery and removal

All mutations require an explicit caller request and context.

| Call | What an acknowledged result establishes |
| --- | --- |
| `Replay(ctx, id, sequence)` | The server returned a real `jobs.Handle` with a nonzero job UUID |
| `ReplayPartition(ctx, id, sequence, partition)` | The partition replay command was acknowledged; this RPC returns no job ID |
| `RetryPartition(ctx, id, sequence, partition)` | One of `RecoveryStarted`, `PartitionNotFound`, `ObserverQuarantined`, `PartitionQuarantined` |
| `ClearPartitionQuarantine(ctx, id, sequence, partition, retryImmediately)` | The clearing outcome; retry outcome is meaningful only when requested and the fence was cleared |
| `ClearObserverQuarantine(ctx, id, sequence)` | The observer-level clearing command was acknowledged |
| `ClearFailedPartitions(ctx, id, sequence)` | The failure-clearing command was acknowledged, not the underlying problem repaired |
| `Remove(ctx, id)` | Resolve the stored sequence, then return `Removed`, `ObserverNotFound`, `ObserverActive` or `ObserverSubscribed`, with a blocking namespace when supplied |
| `RemoveFrom(ctx, id, sequence)` / `information.Remove(ctx)` | Validate the explicit/snapshot sequence against the current definition before requesting the same store-wide removal |

Supply the observer's declared source sequence for replay and recovery. Go does
not use C# reactor replay's empty-sequence inference. An invalid replay job ID is
an error, not C#'s `JobId.NotSet` fallback.

**Persistent removal is store-wide.** The observer definition is shared across
namespaces. Removal first reads the stored definition through `List` and uses its
actual sequence. Missing, ambiguous, malformed or unsupported definition reads
fail without dispatching removal; an explicit or snapshot sequence mismatch also
fails locally. This intentionally corrects C# `IObservers.Remove`'s unsafe
`event-log` default: the kernel's subscription guard targets the supplied sequence,
but deletion removes the shared definition. With the correct sequence, the kernel
refuses removal when the observer is active or subscribed in **any** namespace.
Read-model data and sink containers remain. The lookup and mutation are not atomic
and do not fence concurrent re-registration; stop declaring applications first.

`store.UnregisterReactor`, `UnregisterReducer` and `UnregisterReadModelReactor`
only stop and join this client's local subscriptions. They do not remove persistent
records. Conversely, `Observers().Remove` does not unregister local declarations.
Stop the declaring application before permanently removing obsolete observers;
otherwise a reconnect or another client can still report them.

## Jobs: acceptance is not completion

| Call on `store.Jobs()` | Semantics |
| --- | --- |
| `List(ctx)` | Immutable job snapshots in server order |
| `Get(ctx, id)` | Search the current list, like C#; `nil, nil` means absent |
| `Steps(ctx, id)` | Immutable steps with their own UUIDs, progress and status transitions |
| `Stop(ctx, id)` / `Resume(ctx, id)` / `Delete(ctx, id)` | Command acknowledgement only |
| `WaitForCompletion(ctx, id, timeout)` | Requires actual `CompletedSuccessfully` status |
| `WaitForTerminalOrAbsent(ctx, id, timeout)` | Return a retained terminal job or `nil` for absence, like C# |
| `WaitForProgressCompleted` / `WaitForProgressStopped` | Require the corresponding server progress flag; completed progress may include failed steps |
| `WaitFor(ctx, id, timeout, predicate)` | Poll a synchronous nonblocking predicate on immutable snapshots |
| `WaitForDeletion(ctx, id, timeout)` | Observe absence only; deletion and automatic cleanup are indistinguishable |
| `WaitForJobs(ctx, typeSubstring, timeout)` | Wait for any jobs, or a case-sensitive type substring |
| `TryFindJobsOfType(ctx, typeSubstring, timeout)` | Empty result on the helper's own timeout; caller/RPC failures stay errors |
| `WaitForNoJobs(ctx, timeout, statuses...)` | Wait until none of the selected statuses remains; empty selects all |

A replay handle supports `ID`, `Get`, `Steps` and `WaitForCompletion` using its
original store and namespace. A `Job` also exposes `Handle()` and `Steps(ctx)`.
UUIDs use `github.com/google/uuid` in the API and the .NET Guid layout on the wire.

`WaitForCompletion` returns `*jobs.AbsentError` if the job is missing. Its `Seen`
field distinguishes initial absence from disappearance after a poll observed it.
Neither proves success: short successful jobs may be automatically removed, but
so may explicitly deleted jobs. `Failed`, `CompletedWithFailures` and `Stopped`
produce `*jobs.TerminalError` carrying the real snapshot. No successful job is
synthesized from counters or disappearance. Unknown numeric job/step statuses and
status-history values remain available for diagnostics. An unknown job status
never counts as terminal or successful; status waits continue until known evidence
or their deadline. Unknown observer types, owners, runtime states and failure kinds
are likewise preserved rather than failing an entire list.

Job waits poll every 50ms, default to five seconds when timeout is zero and honor
`jobs.InfiniteTimeout` without adding a deadline. The caller's cancellation or
deadline always applies. `WaitForNoJobs` deliberately corrects C#'s inverted
status-selection predicate; it does not claim completion of the removed work.

## Wait for appended events to be processed

Use the metadata-bearing append methods to retain original types and generations.
This excerpt assumes `ctx`, a registered `store`, and a registered `OrderPlaced`
event. Check the append outcome before waiting:

```go
func appendAndWaitForOperations(ctx context.Context, store *chronicle.EventStore) error {
    appended, err := store.EventLog().AppendWithMetadata(ctx, "order-42", OrderPlaced{})
    if err != nil {
        return err
    }
    if err := appended.Result().Err(); err != nil {
        return err
    }
    processed, err := appended.WaitForCompletion(ctx, store.Observers(), 0)
    if err != nil {
        return err
    }
    if !processed.IsSuccess() {
        return fmt.Errorf("processing incomplete: timed out=%t, outstanding=%v",
            processed.TimedOut(), processed.OutstandingObservers())
    }
    return nil
}
```

`AppendManyWithMetadata` and `AppendBatchWithMetadata` support the same wait.
Alternatively call `Completion()` and later pass its immutable value to
`Observers().WaitForCompletion`. For prepared batches and units of work, an
existing append notification's `Completion()` retains exact original types and
positions. `observation.NewCompletion` supports adapters that already possess
those confirmed coordinates; do not use it to reinterpret an unknown append.

A mixed batch A/B/A at positions 3/5/8 waits from position 3, with A's tail 8 and
B's tail **5**, not 8. The maximum-position entry supplies each type's generation.
A foreign store or namespace is rejected; the request uses the original sequence.
The legacy mutable `CompletionTarget` alone lacks generations and is therefore
not silently upgraded through the current event catalog.

The default server budget is five seconds, with **200ms client grace** to receive
outstanding-observer diagnostics. `observation.InfiniteTimeout` sends zero server
milliseconds and installs no helper deadline. Caller cancellation/deadline is an
error. Server timeout or expiration of the client grace produces a result with
`TimedOut() == true`; transport failures otherwise remain errors.

An unavailable append tail completes trivially, exposed by `Trivial()`. A
committed append with a nil observer surface returns `*observation.CannotWaitError`.
An unknown append outcome is an error, never trivial success. Processing success
is not a stronger promise about durable sink/checkpoint persistence. The kernel
also returns success when **no matching observers exist**. A successful wait is
not proof that an expected producer was registered, subscribed or ready; establish
producer readiness independently before appending when your workflow requires it.

## Failure boundaries and kernel limitations

Use `errors.As` for `OutcomeUnknownError` from either package. After a dispatched
mutation loses its reply, times out, returns malformed data or reports an execution
exception, the server may already have acted. Inspect state before making another
explicit request. Authorization/validation refusals retain `chronicle.EnvelopeError`;
context and gRPC status identities remain inspectable. No automatic mutation retry
is added. Injected connections must not independently configure unsafe retries.
`OutcomeUnknownError.Error()` prints only the operation and unknown-outcome text;
its wrapped cause remains available for deliberate inspection and may contain
sensitive server diagnostics.

Required message fields, IDs, mutation-outcome enums and contradictory completion
flags are validated; malformed collections fail atomically. Diagnostic enum values
are preserved even when newer than this client. Proto3 scalar defaults and
empty acknowledgement messages cannot establish more than their wire contract:
for example a default removal outcome represents `Removed`, and an empty partition
replay acknowledgement carries no completion evidence.

The pinned 19.29.4-development kernel supports the replay/job, failure-diagnostic
and blocked-versus-completed processing workflows above. Replay jobs can disappear
before a terminal snapshot is read. Existing
[reducer recovery](https://github.com/Cratis/Chronicle/issues/4540) and
[catch-up race](https://github.com/Cratis/Chronicle/issues/4548) defects remain
kernel limitations. The SDK does not clear failures, append replacement events
or replay automatically to conceal them. See the per-feature
[parity map](parity.md#observer-and-job-administration).
