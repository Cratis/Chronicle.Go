---
title: Evolve stored events
description: Keep historical Go event shapes and register adjacent, bidirectional kernel migrations.
---

When an event already has persisted history, keep its old shape and add a new
generation instead of overwriting its schema. The Go SDK describes migrations;
Chronicle applies them when producing generational content. It never runs Go
callbacks against stored payloads.

This how-to targets Chronicle 19.32.3 and assumes you already
[register event types](event-types.md). Before any history exists, simply change
the event rather than maintaining unnecessary migrations.

## Keep the previous shape

The [complete example](../../examples/evolution/main.go) defines
`CustomerRegisteredV1` with `Name` and numeric `Status`, and the current
`CustomerRegistered` with `FirstName`, `LastName`, `Status` and `Kind`.
Its explicit JSON tags keep shared wire names stable.

Give the current event generation two. Register the historical type through the
current handle: its ID is inherited and cannot drift. Both types must be named,
non-pointer structs in the same registry. Historical types can still be appended
or used as handler parameters.

## Describe both directions

This function comes from the example; imports are `chronicle` and `events` from
`github.com/cratis/chronicle.go` and its `/events` package:

```go
func registry() (*chronicle.Registry, error) {
    registry := chronicle.NewRegistry()
    current, err := chronicle.RegisterEvent[CustomerRegistered](registry,
        events.WithID("customer-registered"), events.WithGeneration(2))
    if err != nil { return nil, err }
    previous, err := chronicle.RegisterEventGeneration[CustomerRegisteredV1](registry, current, 1)
    if err != nil { return nil, err }
    err = chronicle.RegisterEventMigration(registry, current, previous,
        events.Migration[CustomerRegistered, CustomerRegisteredV1]{
            Upcast: func(b *events.MigrationBuilder[CustomerRegistered, CustomerRegisteredV1]) {
                b.Split("FirstName", "Name", " ", 0).
                    Split("LastName", "Name", " ", 1).
                    DefaultValue("Kind", "customer")
            },
            Downcast: func(b *events.MigrationBuilder[CustomerRegisteredV1, CustomerRegistered]) {
                b.Combine("Name", " ", "FirstName", "LastName")
            },
            MapValues: func(b *events.ValueMapBuilder[CustomerRegistered, CustomerRegisteredV1]) {
                b.For("Status", "Status", events.ValueMapping{From: int32(1), To: int32(10)})
            },
        })
    if err != nil { return nil, err }
    return registry, nil
}
```

Paths are **Go field names**, including dotted nested fields such as
`Contact.Email`. The compiler resolves their actual serialized names through the
client's shared serialization plan: explicit `json` tags win over naming policy.
It validates every source and target. Collection-element traversal and JSON names
containing literal dots are not object paths and fail construction.

| Builder operation | Meaning |
| --- | --- |
| `RenamedFrom(target, source)` | Copy a source property into a renamed target |
| `Split(target, source, separator, part)` | Select a zero-based string part |
| `Combine(target, separator, sources...)` | Join source properties in order |
| `DefaultValue(target, value)` | Fill an absent property; present-null stays null |
| `MapValues(target, source, mappings...)` | Translate values for this direction only |

Unmentioned properties pass through and the kernel filters them through the
target schema. Unmapped values pass through unchanged. A shared `MapValues`
callback declares previous-to-current pairs once; downcasting inverts them.
When several previous values collapse onto one current value, the first pair
wins the inverse. Directional operations run after shared maps and override the
same target. Defaults and mapped values use JSON encoding; object literals must
already use serialized names. Supported Fundamentals concepts retain their codecs.

Both direction callbacks are required. An empty callback explicitly means no
transformation for that direction. Callbacks run once at registration and their
output is snapshotted; they are not invoked by reads, reconnects or replay.

## Validate and register

Construct the client with the registry and
`chronicle.WithEventTypeGenerationValidation(true)`. `NewClient` rejects missing
endpoints, different IDs, nonadjacent generations, duplicate migrators and invalid
paths without connecting. With validation enabled, it also requires every link
from generation one to current. For generation three, register `1 -> 2` and
`2 -> 3`; an older historical handle can be the upgrade endpoint of the first link.

The kernel receives one registration per event ID, the real schema for every
registered generation, and both JSON transformations for every link. No empty
placeholder schema replaces a historical codec. Kernel validation also protects
persisted schemas from incompatible same-generation changes.

Validation remains disabled by default for C# compatibility. That permits a
current generation above one without a chain, but does not excuse malformed
migration declarations. Enable validation for upgrades; disabling it can overwrite
history's schema. Registries and migrations are frozen per client and per store.

Run the offline example:

```sh
go run ./examples/evolution
```

It prints `customer-registered: 1 -> 2 (upcast and downcast)` without connecting.
The integration suite exercises registration, historical backfill, upcast,
downcast, and observers subscribed to each generation against the kernel.

## Decode reads and observe historical shapes

`events.Decode[T](catalog, appended)` selects the representation for registered
`T`. Obtain the naming-aware catalog from `store.EventTypes()` or
`client.Catalogs(storeName)`. `appended.Decode(catalog)` instead selects the exact
persisted generation and returns a pointer to its registered Go type.

Reactor and reducer methods can accept either current or historical types. Their
subscriptions select that generation; the runtime uses its codec and adjusts the
delivered event context when alternate content is available. A historical concrete
handler remains discoverable on its own, unlike the C# generation-only expansion
omission. Each reactor or reducer must select one generation per event ID,
including live/replay bindings and interface families. Use separate observers to
consume both generations. Go rejects an ambiguous selection instead of relying
on C#'s first matching subscription or decoding into the wrong handler type.

Historical backfill runs asynchronously in the kernel: registration readiness
is not a backfill barrier. When upgrading an existing store, wait for historical
representations before starting observers that require the new shape.

If the kernel supplies no alternate for the requested generation, typed reads and
observers retain C#'s raw-content fallback. They do not manufacture a migration or
change the persisted read context. For strict callers, inspect
`GenerationalContent` before requesting a different shape. Exact catalog lookup
and `appended.Decode` reject unknown generations rather than silently guessing the
latest codec; `LookupID` explicitly selects the current generation.

### Protected events across generations

From Chronicle 19.32.2 ([Chronicle#4456](https://github.com/Cratis/Chronicle/issues/4456))
migrations transform PII and encrypted values as plaintext and protect every
generation for the original subject. The event's current representation, and
projections built from it, release correctly before and after erasure. The
kernel still releases **only** that current representation: other generations
in `GenerationalContent` reach the client as ciphertext. Decoding a protected
event as another generation, or observing it with a historical-generation
reactor or reducer, therefore returns ciphertext in classified fields. Do not
read or observe classified events at a generation other than the one the
kernel returns as current. `TestKernelProtectedEventGenerationMigrationProfile`
records this kernel behavior.

Continue with [reading events](reading-events.md) and the
[behavior-level parity map](../parity.md#event-evolution).
