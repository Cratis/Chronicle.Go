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

The output identifies `customer-registered`, generation `1`, with content `{"name":"Ada"}` because this example's `Name` field has an explicit `json:"name"` tag. `T` and `*T` append values resolve to the same declaration; nil event pointers fail. Register the non-pointer struct type.

## Persisted identities and generations

The default ID is the simple Go type name, matching C#'s simple-name convention. Prefer an explicit `events.WithID` for cross-language events so a refactor cannot rename stored history. IDs are strings, not necessarily UUIDs; comma-containing IDs are rejected because kernel tail filters use comma-separated IDs.

`events.WithGeneration(n)` selects a positive generation (default `1`). Duplicate Go types or current persisted IDs fail deterministically without changing the registry. Historical generation codecs, migration chains, tombstones and compensation declarations are not yet implemented. Each registration sends the current schema in both `Schema` and `Generations`, like C#.

Like the C# client, kernel schema/generation validation is **disabled by default**, so a current generation above `1` can register without migrations. Enable `chronicle.WithEventTypeGenerationValidation(true)` to reject incompatible re-registration of an existing generation. With validation enabled, generations above `1` cannot register until migration authoring is supported. Disabling validation permits overwriting schemas: do not change a generation that already has history. A shared C#/Go event ID also requires compatible schemas, not just matching JSON property names; validation rejects differing generated schemas.

`events.WithTags(...)` adds immutable static tags. Append merges static and dynamic tags distinctly, preserving order. `store.EventTypes()` exposes a frozen catalog for lookup, including downstream Arc event classification.

## JSON and schema rules

One compiled field plan supplies both serialization and the registered schema:

| Go shape | Persisted representation |
| --- | --- |
| Untagged exported properties | Spelling preserved by default (`URLValue` stays `URLValue`); see [naming policies and migration](../serialization.md) |
| `json:"name"` | Explicit persisted property name; recommended across languages |
| Named string, numeric and boolean primitives | JSON scalars with kernel numeric formats, not wrapper objects |
| `time.Time` | RFC 3339 string with `date-time` schema format |
| `uuid.UUID`, `concepts.UUID` | Canonical UUID string with `uuid` schema format |
| `concepts.DateOnly`, `TimeOnly`, `TimeSpan` | Strings with `date`, `time`, `duration` formats |
| Valid Fundamentals `Concept[T]` with standard codecs | Declared scalar schema; codec JSON checked before dispatch |
| Nested structs | Nested objects following the same naming rules |
| String-keyed maps, arrays and slices | Objects/arrays with typed schema members |
| Pointers, including `*time.Time` and `*uuid.UUID` | Nullable schemas; nil properties omitted and distinct from present zero/false |
| Nil map/slice properties | Omitted, matching Chronicle's omit-null profile |
| Zero numbers and false | Preserved unless explicitly tagged for omission |
| `json:"-"`, `omitempty`, `omitzero` | Exclusion or explicit omission; `omitzero` honors `IsZero()` as `encoding/json` does |

Empty non-nil slices remain `[]`; null collection elements and null map values fail serialization. Scalar and array integers use kernel formats (`int64`, `uint64`, etc.), preserving precision above 2^53. The 19.29.4 MongoDB append path parses JSON integers as signed BSON values, so unsigned values above `MaxInt64` return `ErrUnsupported` before dispatch; use strings for that range. Go `int`/`uint` use stable 64-bit schemas; `int8`/`uint16` widen to the kernel's `int16`/`uint32` formats. The 19.29.4 kernel ignores dictionary value schemas and converts all nested numbers through `double`: integer values under any map are restricted to the inclusive range -2^53 through 2^53 (unsigned: 0 through 2^53). Values outside that range return `ErrUnsupported` before dispatch, including integers inside nested structs, pointers and collections. Use typed struct properties or string values for wider dictionary integers. Omission tags control outgoing JSON; non-nullable omitted scalars may materialize as kernel defaults on read-back. Use a separate event for an optional fact rather than a nullable domain property.

Interfaces, recursive shapes, embedded/anonymous fields, byte slices, custom JSON/text marshalers (except supported scalars and valid Fundamentals concepts), complex-key maps and unsupported JSON tag options fail registration. Rich custom-schema codecs, enums, polymorphism and GeoJSON are later work. Every nonempty `chronicle` field tag also fails: this client will not pretend to encrypt PII or silently ignore subject/routing metadata.

Protected fields must not be sent as plaintext through an unsupported classification. [The parity map](../parity.md) tracks the missing compliance and serialization surfaces.
