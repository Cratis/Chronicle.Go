---
title: Append atomic batches
description: Append ordered events across sources with independent concurrency checks and complete outcomes.
---

Use `Sequence.AppendMany` for one source or `Sequence.AppendBatch` for ordered events across sources. Both submit one atomic append within one store, namespace and sequence. A known rejection commits none of the events. These APIs remain experimental in the v0.x SDK.

## Choose the batch shape

| API | Input and behavior |
| --- | --- |
| `AppendMany(ctx, source, values, options...)` | `[]any` of registered events for one source; accepts the same `AppendOption` values as `Append` |
| `AppendBatch(ctx, entries, options...)` | `[]eventsequences.Entry`; preserves input order, including A1, B1, A2 |

An `Entry` carries `Source`, `Event`, `Route`, `Tags`, `NamedTags`, optional `Occurred` and `Subject`, and per-entry `Causation`. Route defaults are `Default` / `All` / `Default`. Subject defaults to the entry's source. Per-entry causation extends the context chain; actor and correlation are batch-wide. The SDK serializes all entries before resolving concurrency or dispatching the write. Do not mutate caller-owned inputs concurrently with the call.

`AppendMany` unions static tags from **all** event types into shared tags for every event. It retains this behavior when a non-default route requires the heterogeneous RPC. `AppendBatch` instead merges each type's static tags, that entry's tags, and batch tags independently. Named tags merge as exact name/value pairs; identical pairs coalesce, while different values under one name remain distinct.

Batch options are `WithBatchCorrelation`, `WithBatchTags`, `WithBatchNamedTags` and `WithScopes`. Options snapshot their slice/pointer inputs when constructed. Scalar options are last-wins; nil options fail.

## Protect the histories that informed your decision

Use `ReadHistory` and pass `history.Scope()` to a `LabeledScope` through `WithScopes`. See [reading history](reading-events.md). Each label must be nonblank and unique. If a scope narrows by `SourceID`, that ID must equal its label. A label need not have an appended event: it can protect a separate decision history while writing another source.

Explicit labels replace the matching source's default check. Otherwise, each distinct source resolves its tail using its **first entry's route**, matching C#. Repeated entries do not create additional checks. To protect several routes for one source, use a broader source-bound scope or independent non-source-bound route filters; do not assume each route is implicitly protected.

Expectations retain the [single-append meanings](appending-events.md): resolve, upper-bound `Exact`, protected absence, and no check. Go also resolves explicitly supplied independent filters. An empty resolved tail is unchecked, not protected absence.

An eventless batch must carry at least one effective protected scope. `AppendBatch(ctx, nil, WithScopes(...))` can validate a decision without manufacturing an event. A completely unchecked empty batch fails locally. Kernels that reject eventless checks return `ErrUnsupported` with the rejected result, never success.

## Inspect the entire result

Both methods return `(BatchResult, error)`. Check the operation error first, then `result.Err()`. Results preserve positions in input order, correlation, every constraint violation, labeled concurrency violations in `SourceID`, unknown future error codes, and per-event-type completion targets. Eventless success has no positions or completion target.

`ConcurrencyCheckPerformed` means **every supplied scope** was checked. It is legitimately false when a batch mixes protected scopes with `NoCheck` or an unchecked empty resolution. The protocol has no per-scope success flags. When all scopes require checks but a committed result reports false, the SDK returns that committed result plus `ErrUnsupported`; do not retry it.

Constraint or concurrency violations mean `Rejected`, even with accompanying errors: no events committed. Errors-only kernel failures mean `Unknown`, preserve every diagnostic, and expose `OutcomeUnknownError` through both the operation error and `result.Err()`. C# exposes `IsSuccess`/`Errors` without a disposition; Go treats errors-only results as unknown because the kernel response cannot distinguish pre-commit from post-commit failures.

A transport failure, malformed response, or execution exception is also an `OutcomeUnknownError`. The SDK never retries an append, splits one batch into separate commits, or claims exactly-once delivery.

## Run the example

The [batches program](../../examples/batches/main.go) loads source A, protects that exact history, then appends A1, B1, A2 in one batch and reads A back. With a local development kernel running as described in [getting started](../clients/go/getting-started.md), run from the repository root:

```sh
go run ./examples/batches
```

Expected output:

```text
Batch positions: [0 1 2]; A history positions: 0, 2
```

Set `CHRONICLE_INTEGRATION_CONNECTION_STRING` to select another development endpoint. The example permits a self-signed certificate and creates a uniquely named store on each run. Use only a disposable development kernel; removing that kernel's disposable storage resets the example data. Production callers must configure validating TLS and credentials.
