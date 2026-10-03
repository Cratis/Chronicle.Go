---
title: Add a projection to an existing store
description: Register a new ordinary read model and projection while retaining existing readers and truthful registration outcomes.
---

Use `store.RegisterProjection(ctx, declaration)` to add a new ordinary projection
and its new read model after obtaining an `EventStore`. The declaration uses the
same `readmodels.Define`, model-bound tags and projection builders as startup.
Its exact event generations must already belong to the selected store.

The [compiling external-caller example](../../example_runtime_projection_test.go)
shows declaration, registration, append and a typed reader. It requires a local
development kernel. The kernel integration test exercises eventual typed
materialization, preservation of earlier definitions, reconnect and acquisition
of a new namespace. With its `StockView` type and already acquired `store`, the
registration step is:

```go
model, err := readmodels.Define[StockView]()
if err != nil {
    return err
}
registration, err := store.RegisterProjection(ctx, projections.ModelBound(model))
```

Inspect both `registration.Published` and `err` before deciding whether to retry.

## Supported additions

This first runtime workflow admits active, unclassified models and inputs with
scalar fields. It rejects existing model types, identifiers, containers or
producer identities, even when a different schema, generation or sink makes a
replacement look like an addition. Variants and globals are rejected from their
authoring metadata, not inferred from their lowered wire representation.

Passive models, relationships, nested/derived shapes, open-ended subscriptions,
external-store subscriptions and protected inputs/models are unsupported. This
API does not add events, constraints, reactors, reducers or other artifact
families. It does not unregister or replace definitions.

## Publication and acknowledgement

`ProjectionRegistration` contains two independent facts:

- `Published` says the validated addition belongs to the retained local desired
  state. Cancellation before publication leaves that state unchanged.
- `Outcome` is the existing `RegistrationOutcome`: actual stage acknowledgements,
  failures and retries. It is not evidence that an observer attached or that
  materialization, replay or evolution finished.

The SDK publishes one coherent store snapshot and then calls
`WaitForRegistration`. If registration fails or the context expires after
publication, the addition remains. Call `WaitForRegistration` again with an
appropriate context; resubmitting the declaration is a collision, not a retry.
Never blindly retry an application append whose outcome is unknown.

With an acknowledged predecessor, the new model is registered before an additive
projection request with explicit `FullSet=false`. A new generation, an uncertain
model stage or accumulated unacknowledged additions instead registers the complete
current model set before the complete projection set. No subset is labeled full.

## Unknown destructive requests

An in-flight cumulative projection request blocks new publication. If dispatch
has an unknown outcome, the client permanently refuses future additions for that
store with `ErrDestructiveRegistrationUnknown`. A later acknowledgement, reconnect
or readback cannot clear that uncertainty. Client recreation is not a proven
remote ordering fence either.

The refusal does **not** make ordinary `Ready`, `WaitForRegistration`, reads or
appends fail after their current registration stages are acknowledged. If a
published addition's cumulative attempt becomes uncertain and a retry succeeds,
the call returns `Published=true`, the real successful outcome, **and** the
sentinel. This signals that later additions are disabled, not that the successful
acknowledgement was withdrawn.

Coordination is local to one client and logical store, shared by all namespaces
and retained even when initial handle acquisition fails. Another client submitting
definitions for the same owner is outside this guarantee. Same-root asynchronous
evolution, replay and repair retain their existing kernel limitations; this is
not a remote quiescence or idempotence guarantee.

## Snapshot and decision ownership

New `ReadModels()` calls use the latest root. Previously acquired readers retain
their original schemas, producers and callbacks. `Client.Catalogs(store)` reports
the latest selected-store snapshot without constructors, namespaces or network
I/O. Other stores sharing the initial registry do not acquire the addition.

Publication invalidates all older issued and enrolled decision guards for that
store across namespaces. An old decision reader cannot mint fresh evidence after
publication; acquire a new reader and make a new decision read. Unrelated-store
guards remain valid. Tokens are still SDK evidence, not authenticated server
proofs or protection against server-side definition/history ABA changes.

Sequence handles, event catalogs, append subscriptions and already prepared batch
bytes, event generations and metadata do not change. Close competes with local
publication and final dispatch admission; work already admitted may invoke the
raw transport after Close starts. Cancellation does not join remote execution.
