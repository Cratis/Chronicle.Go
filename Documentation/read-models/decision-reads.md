---
title: Guard decisions with an owned commit
description: Read admitted projection state and reject stale decisions in one atomic event batch.
---

Use a decision read when events depend on projection state that must not become stale before the append. Enroll the read in a [unit of work](../events/unit-of-work.md), stage the resulting events, and let its owner commit once. A competing matching append rejects the entire batch, including events for other sources.

This API is experimental in the v0.x SDK. It implements **client-issued optimistic guards, not signed or server-authenticated read proofs**. It requires the characterized Chronicle **19.32.3** profile, including session reads, explicit no-match scopes and eventless plain-batch validation. Unknown, older and uncharacterized newer versions return `chronicle.ErrUnsupported`; structural compatibility alone does not establish these behaviors. `WithSkipCompatibilityCheck` supplies no positive evidence and cannot enable decision reads.

## Read, decide and commit

1. Register events and a projection-backed read model before creating the client. Use a schema key named `id` or `Id`, with plain string or UUID representation. A Go `chronicle:"key"` tag alone does not rename the kernel's schema key. Obtain `store.ReadModels()` and `store.EventLog()` from the same client, store and namespace.
2. Create `readmodels.DecisionsFor(store.ReadModels(), model)`. `Admit()` returns a `DecisionReadAdmission` without I/O. Admission describes the frozen local shape; it does not promise server agreement or issue a token.
3. Call `transactions.Begin(ctx, store.EventLog())`. Retain the returned owner in the outer scope, defer its rollback, and share only the participant through `transactions.WithUnitOfWork(ctx, unit)`.
4. Call the decision reader's `Get(ctx, key)`. It returns `DecisionRead[T]` and automatically enrolls its token into the participant. Missing participants return `ErrDecisionRequiresUnitOfWork`; no implicit unit or successor is created.
5. Inspect `read.Instance.Exists` before using `Value`. Stage events through `unit.Stage`. Call `owner.Commit(ctx)`, check the operation error, then `result.Err()` and the disposition. Never blindly retry a completed owner or an ambiguous write.

This excerpt from the [typed example's `rename` function](../../examples/decisions/main.go) uses its existing `ctx`, `unit`, `store`, `model`, `key` and named `result` return value:

```go
reader := readmodels.DecisionsFor(store.ReadModels(), model)
read, err := reader.Get(transactions.WithUnitOfWork(ctx, unit), key)
if err != nil {
    return result, err
}
```

`read.Instance` is an `Instance[Account]`, and the successful call has already enrolled its token. The example creates an account from an absent decision, then reads the same name and completes an eventless decision. With a disposable development kernel as described in [getting started](../clients/go/getting-started.md), run:

```sh
go run ./examples/decisions
```

Expected output:

```text
Created account; unchanged decision validated without appending events
```

Set `CHRONICLE_INTEGRATION_CONNECTION_STRING` for another development endpoint. Each run creates a unique store. The example accepts a self-signed development certificate; production clients need validating TLS and credentials. No external side effect should run on the assumption that staging has already committed.

## Detached reads and token ownership

`GetDetached(ctx, key)` returns the same typed result without enrolling or creating a unit. Later, explicitly call `unit.Enroll(read.Token)` on the same client's target. The first **successful** enrollment permanently binds the token and every copy to that unit's existing owner. Rollback, rejection and completion do not release it for another owner. Failed enrollment changes neither token nor unit.

`transactions.DecisionToken` has no public constructor, setters or deserialization authority. Its zero value is invalid; `IsZero()` identifies an unissued token. Ordinary reads, watches, `LastHandled`, explicit scopes, low-level transports and `chronicletest` cannot manufacture decision evidence. Tokens control SDK enrollment only: they cannot stop a malicious caller from submitting fabricated concurrency scopes through direct RPCs.

| Enrollment failure | Error |
| --- | --- |
| Zero/unissued token | `transactions.ErrInvalidDecision` |
| Other client, store, namespace or sequence | `transactions.ErrDecisionTarget` |
| Token already bound to another owner, including after rollback | `transactions.ErrDecisionOwner` |
| Changed local catalog epoch or connection generation | `transactions.ErrStaleDecision` |
| Explicit colliding or unchecked scope | `transactions.ErrDecisionScope` |
| Completing or terminal unit | `transactions.ErrCompleting` / `transactions.ErrCompleted` |

The owner never appears in context. `GetDecisionConflicts()` maps a rejected result's concurrency labels to enrolled model/key pairs. Treat keys as potentially sensitive when displaying or logging diagnostics.

## Supported projection shapes

Admission requires exactly one known event-log projection, active or passive, and a finite nonempty union of registered `From` and `RemovedWith` event IDs. Comma-containing IDs and unresolved event generations are refused. Source keys must be omitted or `$eventSourceId` for `From`, removal handlers, their parent keys and `All.Key`. Finite root `All` mappings are allowed.

Reducers, other event sequences, joins, removal joins, children, nested projections, open-ended subscriptions, `FromEvery` derivatives and `FromEventProperty` are refused. The actual schema key must be an unformatted string or a string with `uuid`/`guid` format; numeric, absent and other conversion keys are unsupported. A plain nullable string retains its string conversion; nullable format suffixes such as `uuid?` are unsupported. Keys cannot be blank, padded, `*`, or contain `#`. UUID keys must use lowercase, hyphenated canonical text.

Classified models are also refused **before acquiring a lease or issuing any RPC**, even when the model would be absent, empty or zero-valued. `DecisionProtectedModel` covers PII and subject/namespace/global encryption, including nested references, type declarations and frozen providers. Protection metadata errors return `DecisionProtectionMetadata`. Kernel 19.29.4 session replay substitutes the requested source key for event subjects, so a token must not be issued over potentially corrupted state. There is no pre-read or second-decryption workaround.

`DecisionReadRefused` provides a model identifier and stable `Reason`; inspect it with `errors.As`, or match `ErrDecisionReadRefused` with `errors.Is`. Local admission and key failures issue no RPCs.

## Read boundaries and cleanup

Each attempt checks selected server model/projection fields, captures an **unfiltered event-log boundary**, probes matching source/event-type progress separately, then folds through a fresh random session. Admitted models are unclassified: the reader validates the server-released session document and uses ordinary descriptor decoding without a second compliance RPC. A fold behind the probe or ahead of the pre-fold boundary retries with a new session and new tails, at most three attempts. Exhaustion returns `DecisionFoldIncomplete` or `DecisionFoldAhead`, never a token.

Unavailable progress means absent state even if the projection returns initial-state JSON. Removal can return absent state with meaningful `LastHandled`. Neither `LastHandled` nor an observer checkpoint is a protection boundary. An empty log uses an explicit no-match scope, not ordinary unchecked empty-tail resolution.

Every created session is dehydrated before the read returns or retries. Cleanup has a five-second budget detached from request cancellation, retaining request metadata and the original generation. Read, protocol, decode and cleanup failures return no token; read and cleanup errors can be joined. Generation loss cannot move a fold or its cleanup onto the next connection. Cleanup failure may leave server session resources to server lifecycle management; it never becomes successful enrollment.

The generation-pinned lease covers RPC folding, duplicate-JSON/protocol checks, awaited dehydration and both agreement checks. Application codecs run only after that counted lease is released; a codec can synchronously close the client without waiting on its own read. Before issuing a token, the reader rechecks the original caller's context, generation and catalog epoch. Closing the client or invalidating that evidence during decoding returns no model or token. A codec panic returns `DecisionCodecPanicError` without retaining or formatting the panic value, also returning no model or token.

Decision-read error messages omit model data, source keys and server diagnostics, including joined fold/cleanup failures. Use `errors.Is` and `errors.As` for cancellation, unsupported operations, admission refusals and protocol failures. Returned codec errors retain their original causes behind the payload-free `serialization.UnmarshalError`. Unwrapped causes remain inspectable and can contain sensitive server or codec text; do not log them indiscriminately.

## Protected completion

An enrolled decision counts as work even with no staged events. Completion sends **one atomic batch** containing the frozen ordered event snapshots and checked scopes. Eventless completion uses the plain batch RPC, returns no event positions and appends no dummy event. There is no separate validation call followed by an unchecked append.

Multiple decisions for one source merge at the earliest boundary and union their event IDs. Absence sorts before actual positions. Explicit scopes colliding with decision labels are rejected in either enrollment order. Independent explicit `Exact` and `NoMatchingEvent` scopes are allowed; `NoCheck`, unchecked empty resolutions and unresolved `Resolve` scopes are refused. Protected completion does not add ordinary automatic effect-source scopes: only admitted decision scopes and explicit checked independent scopes are sent.

Capabilities, token epochs and generation are checked again before dispatch, including after credential acquisition. A false `ConcurrencyCheckPerformed` flag is not success. A confirmed write can still return `Committed` plus `ErrUnsupported`; do not relabel it as rejected or retry it. Malformed replies and transport loss retain outcome-unknown handling. Owners never retry writes.

## Limits of the guarantee

Before and after each fold, server agreement checks the selected producer, sequence, shape and dependency fields, plus server admission, key schema, the registered model generation, actual sink type/configuration and projection active/rewindable flags. Any server-side protection, including namespace/global encryption or unknown protection metadata, refuses the read even when the local model is unclassified. Malformed or unresolved protection metadata also refuses it. A post-fold mismatch returns no model or token after awaited cleanup. This is **not full client/server mapping equality**. The SDK deliberately has no persistent agreement cache.

The protocol binds neither a definition revision nor a history revision to the read or commit. Definition replacement, an ABA definition change, in-place revision and redaction can race between checks. Rechecking before and after a fold cannot eliminate those races, and a later definition change need not invalidate an already detached token. Local publication epochs and reconnect invalidation are not substitutes for atomic server revision support. Keep definitions and existing history stable while relying on these guards, or use a protocol that supplies that stronger binding.

Compared with C#, Go refuses the third ahead fold, awaits cleanup fail-closed, re-admits server parent/`All` keys, key schema, model generation and protection metadata, permanently binds detached-token ownership, and rejects unchecked mixed scopes. See the [decision parity entries](../parity.md#decision-read-migration) for the exact reference and behavioral coverage.
