---
title: Define event types
description: Register Go structs with stable event identities and matching JSON schemas.
---

Events are past-tense facts. Keep source identity in event metadata, and give each event one purpose. Explicit registration replaces C# attributes and assembly scanning; there are no package-global registrations or `init` hooks.

## Register before constructing the client

Create a `chronicle.Registry`, then call `chronicle.RegisterEvent[T](registry, options...)` for each named struct. Pass the registry through `chronicle.WithRegistry`. The client snapshots declarations at construction; later registry changes do not affect it. `WithRegistryForStore` replaces the default catalog for one store.

The following executable example is `ExampleRegisterEvent` in the root tests (excerpt; `CustomerRegistered` is the struct in [getting started](../clients/go/getting-started.md)):

```go
registry := chronicle.NewRegistry()
eventType, err := chronicle.RegisterEvent[CustomerRegistered](registry,
    events.WithID("customer-registered"))
if err != nil {
    return err
}
content, err := eventType.Descriptor().Marshal(CustomerRegistered{Name: "Ada"})
if err != nil {
    return err
}
fmt.Println(eventType.Ref().ID, eventType.Ref().Generation)
fmt.Println(string(content))
```

The output identifies `customer-registered`, generation `1`, with content `{"name":"Ada"}`. `T` and `*T` append values resolve to the same declaration; nil event pointers fail. Register the non-pointer struct type.

## Persisted identities and generations

The default ID is the simple Go type name, matching C#'s simple-name convention. Prefer an explicit `events.WithID` for cross-language events so a refactor cannot rename stored history. IDs are strings, not necessarily UUIDs; comma-containing IDs are rejected because kernel tail filters use comma-separated IDs.

`events.WithGeneration(n)` selects a positive generation (default `1`). Duplicate Go types or current persisted IDs fail deterministically without changing the registry. Historical generation codecs, migration chains, tombstones and compensation declarations are not yet implemented. Registration leaves kernel schema/generation validation enabled.

`events.WithTags(...)` adds immutable static tags. Append merges static and dynamic tags distinctly, preserving order. `store.EventTypes()` exposes a frozen catalog for lookup, including downstream Arc event classification.

## JSON and schema rules

One compiled field plan supplies both serialization and the registered schema:

| Go shape | Persisted representation |
| --- | --- |
| Untagged exported properties | C#-style camelCase (`URLValue` becomes `urlValue`) |
| `json:"name"` | Explicit persisted property name; recommended across languages |
| Named string, numeric and boolean primitives | JSON scalars, not wrapper objects |
| `time.Time` | RFC 3339 string with `date-time` schema format |
| `uuid.UUID` | Canonical UUID string with `uuid` schema format |
| Nested structs | Nested objects following the same naming rules |
| String-keyed maps, arrays and slices | Objects/arrays with typed schema members |
| Nil pointer/map/slice properties | Omitted, matching Chronicle's omit-null profile |
| Zero numbers and false | Preserved unless explicitly tagged for omission |
| `json:"-"`, `omitempty`, `omitzero` | Exclusion or explicit omission policy |

Empty non-nil slices remain `[]`; null collection elements and null map values fail serialization. Integers retain their full width and never pass through `float64`. Use a separate event for an optional fact rather than a nullable domain property.

Interfaces, recursive shapes, embedded/anonymous fields, byte slices, custom JSON/text marshalers (except the built-in time/UUID cases), complex-key maps and unsupported JSON tag options fail registration. Rich custom-schema codecs, enums, date-only/time-only, polymorphism and GeoJSON are later work. Every nonempty `chronicle` field tag also fails: this client will not pretend to encrypt PII or silently ignore subject/routing metadata.

Protected fields must not be sent as plaintext through an unsupported classification. [The parity map](../parity.md) tracks the missing compliance and serialization surfaces.
