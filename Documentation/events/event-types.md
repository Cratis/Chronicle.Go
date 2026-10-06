---
title: Define event types
description: Register Go structs with stable event identities and matching JSON schemas.
---

Events are past-tense facts. Keep source identity in event metadata, and give each event one purpose. Explicit root registration replaces assembly scanning; field tags and typed options carry model-bound declarations. There are no package-global registrations or `init` hooks.

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

## Inspect catalogs without connecting

`client.Catalogs(storeName)` returns immutable event and read-model catalogs without
connecting, creating a namespace, activating constructors or starting observers.
Use it to classify handler returns or inspect model ownership during application
configuration. The descriptors match connected `EventStore` handles: client naming
policy, producer identifiers and store-bound projection sequences are included.
`WithRegistryForStore` replaces both default catalogs, rather than merging them.
The store name must be nonblank; no namespace is needed. Catalogs remain readable
after client shutdown and describe configuration, not server readiness.

`ExampleClient_Catalogs` in the root tests is an executable offline example.

## Persisted identities and generations

The default ID is the simple Go type name, matching C#'s simple-name convention. Prefer an explicit `events.WithID` for cross-language events so a refactor cannot rename stored history. IDs are strings, not necessarily UUIDs; comma-containing IDs are rejected because kernel tail filters use comma-separated IDs.

`events.WithGeneration(n)` selects a positive generation (default `1`). Duplicate Go types or current persisted IDs fail deterministically without changing the registry. Use `RegisterEventGeneration` and `RegisterEventMigration` to retain historical codecs and describe [bidirectional evolution](evolution.md). Descriptor-only tombstone and compensation schema metadata are supported as described below. Each registration sends the latest `Schema` and all concrete schemas in `Generations`, like C#.

Like the C# client, kernel schema/generation validation is **disabled by default**, so a current generation above `1` can register without migrations. Enable `chronicle.WithEventTypeGenerationValidation(true)` to reject incompatible re-registration of an existing generation. With validation enabled, `NewClient` requires a complete adjacent migration chain starting at generation one. Disabling validation permits overwriting schemas: do not change a generation that already has history. A shared C#/Go event ID also requires compatible schemas, not just matching JSON property names; validation rejects differing generated schemas.

`events.WithTags(...)` adds immutable static tags. Append merges static and dynamic tags distinctly, preserving order. `store.EventTypes()` exposes a frozen catalog for lookup, including downstream Arc event classification.

## Model-bound event metadata

These declarations are experimental in the v0.x SDK. Register event structs normally;
`NewClient` compiles their metadata before connecting, independently for each store
registry. Syntax, unsupported directives and ignored-field declarations can fail
already at registration. Errors are inspectable with
`errors.As(err, &declaration)` for `*declarations.DeclarationError`; default error
text omits literal contents.

From `AccountRegistered` in the [executable example](../../example_declarations_test.go):

```go
type AccountRegistered struct {
    Email string `json:"email" chronicle:"unique(name=\"email\",message=\"Email already registered\",sequences=[\"event-log\"])"`
    Owner *string `json:"owner" chronicle:"subject"`
}
```

- `unique(...)` declares property uniqueness; [constraint declarations](constraints.md#model-bound-constraints)
  explain grouping, composites, removal and explicit composition.
- `subject` selects one top-level scalar field, including named scalars, UUIDs and
  supported Fundamentals concepts. Pointers are allowed; nil or empty means absent.
  Multiple subject fields, collection/object subjects and nested subject tags fail.
- Subject precedence is explicit append subject → explicit `WithSubjectResolver`
  → tagged field → append source ID. An explicit resolver returning `false`
  goes directly to the source fallback, not the tag. There is no `ID` field fallback.
  Empty subjects from an explicit resolver or append override still fail validation. Resolution occurs
  once when staging, not again at commit. Inspect the compiled resolver through
  the client's catalog; registration handles precede this compilation.

| Type option | Registration meaning |
| --- | --- |
| `events.WithUnique(events.Unique{...})` | Per-source event-type lifecycle uniqueness; distinct from property uniqueness |
| `events.WithRemoveConstraints(names...)` | Additive release events for named model-bound constraints |
| `events.WithTombstone()` | Records descriptor-only metadata (`IsTombstone()`); wire flag stays false like C#'s inert attribute; does not erase data |
| `events.WithCompensationFor(eventHandle)` | Adds top-level `compensationFor` schema metadata with the target's persisted ID; no compensation execution |
| `events.WithSourceStore(name)` | Declares source-store provenance, including the registration's `EventStore` field and existing observer inbox inference; never routes appends |
| `events.WithTags(tags...)` | Static append labels, merged distinctly with dynamic tags. Repeated options are last-wins; inputs are copied |

Compensation targets must be registered in the same store catalog. The 19.29.4
registration contract has no tags field: static labels are carried by append
requests, not invented registration metadata. Event source/stream classifications
remain explicit append or observer options, not event-field routing tags.

`pii`, `compliance-details(...)` and `encrypted(...)` feed the shared
[compliance schema pipeline](../compliance.md), including nested/type metadata
through `events.WithProtection`. A subject tag is identity metadata, not encryption.

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
| `[]byte`, including named slices over built-in `byte` | Standard base64 string with `byte-array` format; see [binary values](../serialization.md#binary-values) |
| String-keyed maps, arrays and other slices | Objects/arrays with typed schema members |
| Pointers, including `*time.Time` and `*uuid.UUID` | Nullable schemas; nil properties omitted and distinct from present zero/false |
| Nil map/slice properties | Omitted, matching Chronicle's omit-null profile |
| Zero numbers and false | Preserved unless explicitly tagged for omission |
| `json:"-"`, `omitempty`, `omitzero` | Exclusion or explicit omission; `omitzero` honors `IsZero()` as `encoding/json` does |

Empty non-nil byte slices become `""`; other empty non-nil slices remain `[]`. Null collection elements and null map values fail serialization. Scalar and array integers use kernel formats (`int64`, `uint64`, etc.), preserving precision above 2^53. The 19.29.4 MongoDB append path parses JSON integers as signed BSON values, so unsigned values above `MaxInt64` return `ErrUnsupported` before dispatch; use strings for that range. Go `int`/`uint` use stable 64-bit schemas; `int8`/`uint16` widen to the kernel's `int16`/`uint32` formats. The 19.29.4 kernel ignores dictionary value schemas and converts all nested numbers through `double`: integer values under any map are restricted to the inclusive range -2^53 through 2^53 (unsigned: 0 through 2^53). Values outside that range return `ErrUnsupported` before dispatch, including integers inside nested structs, pointers and collections. Use typed struct properties or string values for wider dictionary integers. Omission tags control outgoing JSON; non-nullable omitted scalars may materialize as kernel defaults on read-back. Use a separate event for an optional fact rather than a nullable domain property.

Collections containing binary byte slices, interfaces, embedded/anonymous fields, custom JSON/text marshalers (except supported scalars and valid Fundamentals concepts), complex-key maps and unsupported JSON tag options fail registration. Concrete recursive schemas are supported unless the graph contains binary. Binary-containing types require simple ASCII (letter first, then letters, digits or underscores; no dots), case-insensitively unique, non-reserved property names; see [binary values](../serialization.md#binary-values). Rich custom-schema codecs, enums, polymorphism and GeoJSON are later work. Unknown or role-incompatible `chronicle` tags fail: this client will not pretend to encrypt PII or silently ignore routing metadata.

Protected fields must not be sent as plaintext through an unsupported classification. [The parity map](../parity.md) tracks the missing compliance and serialization surfaces.
