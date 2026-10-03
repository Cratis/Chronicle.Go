---
title: Enforce unique event constraints
description: Declare unique values and event-type lifecycles, release claims, and inspect append-time violations.
---

Use constraints when a value or event lifecycle must remain unique even when writers race. Chronicle checks the rule inside the append; a client-side lookup cannot replace it. Constraint APIs are experimental in the v0.x Go SDK.

## Model-bound constraints

Put `chronicle:"unique"` on a top-level event field, or specify
`unique(name="email",message="Already used",sequences=["event-log"])` inside the
Go struct tag (escape the inner quotes). Register the event normally; no
`AddConstraint` call is needed for its tags. See the
[event declaration example](event-types.md#model-bound-event-metadata).

Declarations with the same name across registered events become one constraint.
Names default to the **Go field name**, not the serialized name; use stable
explicit names for cross-language events. Paths come from the same serialization
plan as the payload, including JSON overrides and the client's naming policy.

Same-name fields on one event form an ordered composite. This is a deliberate
Go improvement: the referenced C# provider calls `On` repeatedly for that event,
which its builder rejects. Go calls the existing builder once per event with all
its fields; it does not add another uniqueness engine. A duplicate `unique`
directive on a single field still fails.

For per-source lifecycle uniqueness, register with
`events.WithUnique(events.Unique{Name: "account-lifecycle", Message: "Already open"})`.
The empty name defaults to the Go type name. Events sharing that name are mutually
exclusive until a remover occurs. Only one type-level Unique declaration is allowed.
Property and lifecycle constraints cannot share a name.

Register a removal event with `events.WithRemoveConstraints("email", "account-lifecycle")`.
Every matching remover is attached through `RemovedWith`; calls accumulate and
duplicate names coalesce. Unknown names fail at `NewClient` rather than silently
failing to release a claim. For explicitly built constraints, use `RemovedWith`
directly instead.

Sequence restrictions merge in declaration order; **any unrestricted declaration
makes the whole constraint unrestricted**. Property messages use the first nonempty
message in registration/field order. Lifecycle messages select the violating
event's nonempty message, then the first nonempty message in that group; no message
preserves kernel text. Blank sequence IDs fail rather than disappearing silently.

`AddConstraint` continues to reject duplicate explicit names. An explicit definition
colliding with a tag-derived name fails with `DeclarationError` at `NewClient`.
Use `registry.ConfigureDeclaredConstraint(name, func(*constraints.Builder))` to
explicitly compose a derived constraint with `IgnoreCasing`, scope, additional
`On` events or `RemovedWith` events. The callback runs once per client snapshot,
using declaration paths, then naming-policy rebinding applies to the result.
Do not perform I/O or mutate the registry in that callback. Renaming the constraint,
foreign event descriptors and unknown composition names fail atomically.

## Declare and register a unique value

Register participating events first, build the definition, then call `Registry.AddConstraint` before constructing the client with `WithRegistry`. The client registers constraints after event types and waits for acknowledgement before returning a store or dispatching an append. Reconnection replays those registrations.

This excerpt from the [executable example](../../examples/constraints/main.go) uses its `EmailReserved` event (`Email string`, tagged `json:"email"`) and empty `EmailReleased` event. It reserves the address across event sources, ignoring casing, until the owning source appends a release event:

```go
func declarations() (*chronicle.Registry, error) {
    registry := chronicle.NewRegistry()
    reserved, err := chronicle.RegisterEvent[EmailReserved](registry, events.WithID("email-reserved"))
    if err != nil {
        return nil, err
    }
    released, err := chronicle.RegisterEvent[EmailReleased](registry, events.WithID("email-released"))
    if err != nil {
        return nil, err
    }
    unique, err := constraints.UniqueValues("UniqueEmail").
        On(reserved.Descriptor(), "email").
        IgnoreCasing().
        RemovedWith(released.Descriptor()).
        WithMessage("That address is already reserved ({PropertyName}).").
        Build()
    if err != nil {
        return nil, err
    }
    if err = registry.AddConstraint(unique); err != nil {
        return nil, err
    }
    return registry, nil
}
```

The example imports `chronicle`, `constraints`, and `events` from `github.com/cratis/chronicle.go`. Go scans field declarations only on explicitly registered roots and has no package-global registry. Both `WithRegistry` and `WithRegistryForStore` take frozen snapshots at `NewClient`; adding definitions later does not update an existing client. A store-specific registry replaces the default one, including constraints. `EventStore.Constraints()` returns a defensive copy of its definitions.

## Choose the constraint kind

| Builder | Meaning | Name default |
| --- | --- | --- |
| `constraints.UniqueValues(name).On(event.Descriptor(), paths...)` | The ordered property combination is unique across sources; its owner may reclaim it | Explicit nonblank name required |
| `constraints.UniqueEventTypes(first.Descriptor(), others...)` | At most one covered occurrence per source in an open lifecycle, even across different covered types | First event's persisted type ID; override with `WithName` |

For a composite key, pass several paths in **one** `On`, such as `"tenant", "email"`. A second `On` adds another event type to the same constraint; its corresponding paths may have different names. Calling `On` twice for the same event ID is invalid. Use exact serialized JSON names, including `json` tags; nested paths use dots, such as `"contact.emailAddress"`. Paths are checked against the event schema, not guessed from Go field names.

Group mutually exclusive event types in one `UniqueEventTypes` declaration. Registry names must be unique; separate same-named definitions are not implicitly merged. Empty names, missing events or properties, invalid paths and foreign/unregistered descriptors fail with `ErrInvalidConfiguration`. Build returns an immutable definition; the builder itself is not concurrency-safe.

## Release and scope claims

`RemovedWith(descriptors...)` applies to both kinds. Repeated calls are additive and duplicate IDs coalesce. Any declared remover releases the **appending source's** claim or lifecycle. Another source's removal cannot release the owner's claim. Removing a claim does not erase historical events.

| Builder method | Behavior and default |
| --- | --- |
| `IgnoreCasing()` | Opts into the kernel's case-insensitive property comparison; default is case-sensitive. Values are not trimmed. Invalid for event-type constraints |
| `PerEventSourceType()` | Adds the appending event's source type to the claim's scope |
| `PerEventStreamType()` | Adds its stream type |
| `PerEventStreamID()` | Adds its stream ID |
| `ForEventSequences(ids...)` | Restricts validation and indexing; repeated calls union IDs in declaration order |
| `ForEventLog()` | Adds `event-log` to that selection |

Scope dimensions combine; omission adds no extra dimensions. Source ID identifies the owner, not an optional dimension. Every namespace and sequence has separate constraint state. **No sequence selection means all sequences**, including outbox, rather than just the event log. Scope and sequence selection also govern removers.

The kernel owns batch semantics. A rejected [atomic batch](batches.md) persists none of its events. Event-type lifecycle removals can open a new cycle for later events in the same batch; do not assume property-value claims have identical intermediate-release semantics.

## Handle violations without retrying blindly

Check the operation error first, then `result.Err()`. `Append`, `AppendMany`, and `AppendBatch` return known constraint rejection as `Disposition == eventsequences.Rejected`, with no committed positions. Use `errors.As(result.Err(), &constraintError)` with `*eventsequences.ConstraintError`, or inspect `result.ConstraintViolations` directly.

Each violation preserves `ConstraintName`, `Type`, `EventTypeID`, `SequenceNumber`, `Message`, and the complete `Details` map, including unfamiliar future keys. A composite conflict can produce one violation per property. `constraints.PropertyName` and `constraints.PropertyValue` identify the standard property detail keys. The sequence number identifies the conflict reported by the kernel; it is not a newly committed position.

`WithMessage(template)` or `WithMessageProvider(func(constraints.Violation) string)` supplies client-side text. The last call wins. Exact `{DetailKey}` placeholders are replaced with detail values; unknown placeholders remain. Empty text preserves the kernel message. Providers run once per matching returned violation, may run concurrently, and receive copied details. They must synchronize captured state. Detail values are inserted literally, never interpreted recursively as templates. Built-in violations without a matching definition keep their kernel messages.

Details may contain identifying values. Avoid logging them or putting `{PropertyValue}` in public messages unless your application deliberately permits that disclosure. A transport or execution error can mean an **unknown** write outcome, not constraint rejection; see [append results](appending-events.md). Never retry an unknown outcome blindly.

## Run the example

With a disposable development kernel running as described in [getting started](../clients/go/getting-started.md), run from the repository root:

```sh
go run ./examples/constraints
```

Expected output:

```text
Competing claim rejected: UniqueEmail
Owner released; new claim committed
```

The program also verifies that the rejected event was not persisted. Set `CHRONICLE_INTEGRATION_CONNECTION_STRING` to select another development endpoint. It creates a uniquely named store each run and permits a self-signed development certificate. Reset data by removing only your disposable kernel's storage; use validating TLS and explicit credentials in production. See [parity and limitations](../parity.md) for C# translation differences and the supported kernel baseline.
