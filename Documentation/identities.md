---
title: Rename a stored identity display name
description: Request one namespace-scoped name update and distinguish acknowledgment from observed storage state.
---

Use `store.Identities().Rename` to change a stored identity's display name without
changing its opaque subject or username. This experimental v0.x workflow performs
one pre-list, one rename command, and one post-list. It does not register an
identity: ordinary actor appends establish identities first.

## Prerequisites

- Use an already-connected client and a fully registered current store/namespace
  and definition root. A fresh successful `Client.EventStore` ordinarily provides
  this readiness. `Rename` never connects, waits for registration, starts observers,
  or joins an incomplete registration pass. After reconnect or definition changes,
  establish readiness separately before a new invocation.
- Your credentials must permit both identity listing and rename. Choosing a
  namespace or an audit actor grants no permission.
- Supply your own context deadline. There is no default timeout, retry, polling,
  compensation, or timeout extension.

The [compiling external example](../example_identity_rename_test.go) shows the
public workflow and error handling. With an acquired ready `store` and `ctx`:

```go
result, err := store.Identities().Rename(
    ctx, "person-42", identities.Name("Jane Austen"),
)
```

Subject and name must be valid UTF-8 and nonblank. Accepted strings remain exact:
no trimming, normalization, case folding, username alias, or event-source lookup.
`identities.Name` is a Go typing aid; C# takes a string. Supply zero or one optional
`metadata.CorrelationID`. Explicit zero generates a fresh ID without consulting
the correlation provider.

## Interpret the result

| Disposition | Meaning |
| --- | --- |
| `IdentityRenameNotDispatched` | This invocation entered no rename RPC. A failed pre-list is not absence. |
| `IdentityRenameRefused` | A structurally valid command envelope explicitly refused authorization or validation, without execution uncertainty. |
| `IdentityRenameUnknown` | A dispatched command or its confirmation is uncertain. Do not blindly retry. |
| `IdentityRenameObserved` | The single post-list contained exactly one matching subject with exactly the requested name. |

`Acknowledged` means the command returned a valid successful envelope, **not** that
a row changed. It remains true when cancellation, root/generation changes, a denied
post-list, or a mismatched name prevents confirmation. Request correlation is
selected once; response correlation preserves command-wire presence and need not
match the request. Results contain no identity records or diagnostic payloads.

A successful empty pre-list returns `*identities.NotFoundError` and
`identities.ErrNotFound`, with zero commands. Invalid input exposes
`chronicle.ErrInvalidConfiguration`; malformed replies expose `chronicle.ErrProtocol`.
Explicit command refusal exposes `chronicle.ErrIdentityRenameRefused`; uncertain
outcomes expose `chronicle.ErrIdentityRenameUnknown`. Known cancellation/deadline
sentinels remain inspectable alongside unknown outcomes. `IdentityRenameError`
provides fixed `Phase()` and `Reason()` categories. Audit provider failures retain
the existing payload-free preparation contract.

## Concurrent renames

A rename holds the store's definition flight for its command. A second concurrent
`Rename` on the same store, in any namespace, is not dispatched: it returns
`IdentityRenameNotDispatched` with reason `registration_not_ready`. Retry after the
first rename completes.

## Scope, cost, and privacy

The server has no filtered or paginated identity listing for this route. Success
requires two O(namespace identity count) reads of **whole-namespace plain identity
strings**, including unrelated records. They enter the client process but are not
returned or logged by this API. Configured gRPC message bounds apply: an oversized
or denied pre-list prevents mutation; the same failure after acknowledgment returns
unknown. The identity storage contract has no PII-release mechanism; this operation
does not invoke read-model release or event-schema decryption.

The SDK freezes actor, correlation, and causation once for this invocation. Rename
has no actor or causation command fields: those values remain contextual, not
persisted command attribution. Correlation travels through the existing header.
No event enricher, subject resolver, origin resolver, concurrency strategy, append
notification, event catalog lookup, or event append runs.

For SDK processing attributable to this `Rename`, unknown borrowed error hooks are
not traversed and borrowed error/panic values and server diagnostics are discarded.
Token acquisition and invalidation run outside locks and RPC work leases. This
boundary does **not** cover separate `Ready`/registration/reconnect/observer calls,
independent background work, borrowed implementations' own logging, or explicit
reentrant operations performed by application callbacks. Calling `Ready` then
`Rename` does not turn both into one protected invocation.

The kernel updates identity storage directly and ignores MongoDB's matched count.
Observation is not an atomic receipt or proof that this command caused the name:
external writers can intervene between steps. No event-source, stream, partition,
subject, or username moves. The pinned single-kernel storage witness does not prove
historical actor-cache or multi-node convergence, classified identity metadata,
SQL behavior, or authenticated role isolation. See the [parity map](parity.md).
