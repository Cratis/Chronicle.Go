---
title: Enrich outgoing events and select audit metadata
description: Add declared event information and per-call audit providers without changing schemas, source events, or staged snapshots.
---

Use `chronicle.WithEventEnrichers` to populate declared event properties just
before an append snapshot is created. Use the three metadata providers to select
an actor, correlation, and causation from the **current call**, not a context
captured when the client was constructed. These APIs do not authenticate callers.

The compiled external examples in
[`example_enrichment_test.go`](../../example_enrichment_test.go) show immediate
append and owner-controlled unit-of-work staging.

## Client options

| Option | Default and repeated options |
| --- | --- |
| `WithEventEnrichers(...events.EventEnricher)` | None. Calls accumulate in order; duplicates execute again. Any nil entry fails construction. |
| `WithIdentityProvider(metadata.IdentityProvider)` | Context actor, otherwise `identities.NotSet()`. Last wins; nil disables. |
| `WithCorrelationProvider(metadata.CorrelationProvider)` | Fundamentals-compatible context correlation, otherwise a new UUID. Last wins; nil disables. |
| `WithCausationProvider(metadata.CausationProvider)` | Context chain. Last wins; nil disables. |
| `WithRootCausation(metadata.RootCausation)` | No implicit root. Last configuration wins. |

Each metadata provider returns a value, a `handled` flag, and an error. An error
or panic fails the operation; it never asks for fallback. A handled actor always
wins, including `Unknown`, `NotSet`, or `System`. `metadata.IdentityFrom` exposes
context presence, so an explicitly installed `NotSet` is not confused with absence.

An explicit `WithCorrelation` or `WithBatchCorrelation` bypasses both the
correlation provider and the parent context. Explicit zero means generate a new
UUID. A provider's handled zero also generates a new ID, rather than inheriting
the parent or a unit's bound ID.

A handled causation chain replaces the entire context chain; handled empty masks
the parent. An opted-in local `Root` is prepended only when the selected chain has
no `Root` already. Entry causes follow in their existing order. Returned identity
chains and causation slices/maps are copied before another callback runs.

## Declared-field editing

An enricher receives `(context.Context, events.TypeRef, *events.EventContent)`.
Its context contains the selected, copied audit values. The original event is
fully encoded before the first enricher. All enrichers for one event finish
before the next event begins; a mixed batch retains A1/B1/A2 order.

- `Get(name)` returns an owned `json.RawMessage`, presence, and an error.
- `Set(name, value)` requires the field's declared Go type, or a registered
  implementation of its declared derived interface. It uses the same frozen
  codec, integer limits, naming, omission, and cycle checks as normal encoding.
- `Remove(name)` requires the compiled encoder to support omission. Schema
  nullability alone is not permission to remove a required property.

Names are exact top-level serialized names, after tags and naming policy: not Go
field names, dotted paths, or arbitrary new schema properties. A replacement
keeps its position; adding an omitted field appends it. Remove followed by Set
also appends. Untyped nil and raw JSON injection are rejected. A correctly typed
nil follows normal object omission. Setter inputs are encoded and owned before
Set returns; the original Go event is never changed.

Each callback receives a fresh handle over the same logical document. The handle
expires when that callback returns, fails, or panics. Retaining it cannot change
the next provider's document or a staged snapshot. Ignoring a Set/Remove error
still fails the operation. This is not a general JSON document editor or an
`AdditionalSchemaFields` escape hatch.

## Subject and confidentiality boundaries

The compliance subject is resolved before enrichment. A field tagged `subject`
cannot be set or removed, even when an explicit append subject is supplied. An
active custom subject resolver has opaque dependencies: its content is read-only
unless an explicit append subject bypasses the resolver. Read-only enrichers
remain usable. No enriched payload is decoded to rerun subject discovery.

Existing protection classifications stay frozen. Append protection is unchanged.
Revision, protected or not, enriches once and sends the selected actor/causes;
the kernel protects revised content with the original event's subject. Its
selected correlation is available to enrichers, but the pinned kernel assigns a
fresh correlation to the revision system event; the revision RPC has no explicit
correlation field.

## Preparation, lifetime, and failure

Validation and complete content preparation precede concurrency strategies, tail
reads, and append dispatch. A failed later event produces no append, notification,
or staged enrollment. `transactions.Begin` binds actor/correlation once without
enriching an empty event list. Stage selects current metadata outside its lock,
rejects conflicting actor/correlation before content mutation, and atomically
enrolls its entire snapshot. Missing correlation inherits Begin's value. Commit,
merge, and reconnect never rerun providers or serialization.

SDK reactor appenders prepare every built-in returned action before executing
any custom effect or append. Existing single, legacy-many, and heterogeneous
routes remain distinct. An empty returned batch with no potentially protected
scope fails preparation before custom effects; empty unit enrollment and
protected eventless checks remain supported. Borrowed custom appenders retain their existing API;
they cannot provide this private SDK preparation guarantee. Several effects or
RPCs are not a rollback-capable transaction.

Callbacks are borrowed for the client lifetime, synchronous, and potentially
concurrent. No request context is retained, and no DI discovery or ambient global
state is introduced. They run outside SDK state locks and work/RPC leases. An
immediate caller's provider may close its client; the pending write then fails
locally. This does not permit self-joining shutdown from an SDK-owned observer
callback. Join all outstanding caller-owned preparation before disposing provider
dependencies. Keep providers metadata-only: the SDK cannot undo application I/O.

`events.PreparationError` (also `metadata.ProviderError`) reports a fixed phase,
zero-based provider/event indices (`-1` when not applicable), and a panic flag.
Application errors and panic objects are discarded without formatting, error
traversal, wrapping, or diagnostic retention. This boundary applies to base
outgoing codecs even with no enrichers or audit providers configured; unsupported
base encoding retains the SDK's `ErrUnsupported` category. Standalone
`Descriptor.Marshal` keeps its ordinary codec error-inspection contract.
Actual caller cancellation is
returned separately; returning `context.Canceled` while the context is live is a
provider failure. The immediate origin resolver still receives the original
caller context; unit completion keeps its existing resolver bypass.

## Opt-in root facts

`metadata.RootCausation` accepts application `SoftwareVersion`, `SoftwareCommit`,
and `ProgramIdentifier`; empty values become `N/A`. Each client captures one
UTC timestamp, Go client build identity when available, and OS/architecture facts.
A dependency replacement path or the application's VCS revision is never treated
as the SDK commit. Unavailable SDK identity is `N/A`.

`IncludeMachineName` and `IncludeProcessID` are separate explicit opt-ins. Requested
machine information that cannot be obtained fails configuration. Arguments,
environment variables, and credentials are never captured. Application-provided
facts must also remain free of secrets and personal data.

These defaults deliberately differ from C#: existing Go clients still default to
NotSet, a fresh missing correlation, and **no** implicit process root.

## Identity-name operations

[Identity rename](../identities.md) resolves these audit providers once before its
pre-list, command, and post-list, outside locks and work leases. An explicit optional
correlation value wins; explicit zero generates fresh without consulting its provider.
The same frozen context and caller deadline reach all three RPCs. Rename has no actor
or causation fields, so selecting those values does not persist them on the command;
correlation uses the existing header. Enrichers, subject/origin resolvers, concurrency
strategies, and append notifications do not run. Rename requires existing current-root
readiness rather than starting or joining registration. Separate readiness operations
and independent background activity are outside its invocation-local privacy contract.
