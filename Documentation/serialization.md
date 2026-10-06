---
title: JSON naming and shared scalar values
description: Choose stable Chronicle property names and serialize Fundamentals.Go concepts without changing their wire types.
---

Chronicle.Go compiles property names, schemas and JSON from one immutable plan.
The default now preserves Go field spelling, matching the C# client's
`DefaultNamingPolicy`. This changes untagged fields from earlier Go versions.

For explicitly registered interface implementations, see
[unprotected derived codecs](derived-codecs.md). Their immediate property names
have a C# camelCase exception, while nested ordinary objects retain the policy
below. For a closed, declared-value Int32 enum or flags table, see
[declared enum codecs](enum-codecs.md); this is opt-in, not string-enum discovery.

## Property naming reference

Select one policy per client with `chronicle.WithNamingPolicy(policy)`.
It applies to every store registry supplied to that client and freezes at
`NewClient`. Reusing a registry with another client does not mutate either client
or the declaration handles. Invalid policies and duplicate serialized names fail
construction before network I/O. Repeated policy options are last-wins.

| Policy in `serialization` | `Person` | `URLValue` | `ID` |
| --- | --- | --- | --- |
| `PreservePropertyNames` (default) | `Person` | `URLValue` | `ID` |
| `CamelCase` (C# Fundamentals) | `person` | `URLValue` | `ID` |
| `LegacyGoCamelCase` (Go compatibility only) | `person` | `urlValue` | `id` |

Explicit `json` tags always win, including inside nested structs and collections.
For a shared C# `Id` / Go `ID` property, use `json:"Id"`; preserving spelling does
not translate different source-language names. The executable
[`ExampleNamingPolicy`](../serialization/example_test.go) shows all three policies.

Container naming is separate: read-model containers remain case-preserving,
pluralized names unless explicitly overridden. Property policies do not change
model identifiers, event IDs or dictionary keys.

Event/read-model schemas and payloads, read-model indexes and subject/PII paths,
constraint paths and projection source/target paths all use plan metadata.
Author paths with the **declaration's** serialized names (the preserving default,
including explicit tags). Client construction resolves those paths by field
identity into the selected client plan; it does not recase path strings or
kernel context expressions. Declaration `Descriptor().Marshal` uses declaration
names; client operations use client descriptors. `serialization.RebindPath` is
also available to future migration tooling; migration authoring is not implemented.

C# Fundamentals' acronym-friendly policy is not System.Text.Json's built-in
camelCase. The source and golden fixtures are pinned in [the parity map](parity.md).
Go projection paths honor explicit tags throughout; some C# reflected projection
paths consult only its naming policy, not `JsonPropertyName`.

## Upgrade an existing Go store safely

For already-persisted Go schemas, retain the previous algorithm explicitly:

```go
client, err := chronicle.NewClient(
    chronicle.WithRegistry(registry),
    chronicle.WithNamingPolicy(serialization.LegacyGoCamelCase),
    chronicle.WithEventTypeGenerationValidation(true),
)
```

This is a client-construction excerpt; `registry` contains your declarations.
Use `LegacyGoCamelCase`, **not** `CamelCase`, when preserving old acronym names.
Update authored paths to the declaration names, or add explicit stable `json` tags;
the client converts paths to the selected wire names from the field plan.

Never overwrite a same-generation schema or rewrite stored content to adopt a
new naming default. Keep existing names until a separately planned generation
migration is available. Generation validation remains disabled by default, like
C#; enable it when connecting to existing history so incompatible registrations
are rejected. Naming agreement alone does not establish schema compatibility:
formats, required properties and nullability must also agree.

## Fundamentals.Go scalars and concepts

Import shared values from `github.com/cratis/fundamentals.go/concepts`.

| Type | JSON | Chronicle schema format |
| --- | --- | --- |
| `concepts.UUID` | Canonical lowercase dashed UUID | `uuid` |
| `concepts.DateOnly` | `"2026-01-02"` | `date` |
| `concepts.TimeOnly` | `"03:04:05.1234567"` | `time` |
| `concepts.TimeSpan` | `"1.02:03:04.1234567"` | `duration` |

`TimeSpan` uses .NET constant format, not ISO-8601 duration syntax. UUID bytes stay
in RFC order; Chronicle's protobuf adapter alone performs BCL Guid conversion.
Existing `google/uuid.UUID` and `time.Time` event shapes remain supported.

A domain concept implements `concepts.Concept[T]` plus value-receiver JSON/text
marshalers and pointer-receiver JSON/text unmarshalers. See Fundamentals'
[concept authoring contract](https://github.com/Cratis/Fundamentals.Go/blob/v0.1.0/Documentation/concepts.md).
Named primitives without custom codecs still work without a marker.

Registration calls `concepts.Underlying` on type metadata, never `ConceptValue`
or codecs on a fabricated value. Invalid declarations return
`chronicle.ErrInvalidConfiguration` wrapping `concepts.ErrInvalidConcept` and an
inspectable `*concepts.TypeError`. Serialization calls the declared JSON codec,
then `concepts.CheckJSON` before dispatch. Unsupported custom codecs remain rejected.
Declared domain types remain in field metadata: two IDs sharing UUID storage are
not interchangeable typed field/key declarations.

Pointers preserve nullable formats (`uuid?`, `date?`, and so on); nil properties
are omitted. Concepts work in slices and string-keyed map values. Missing, zero
and `omitzero` retain the [event serialization rules](events/event-types.md).
Unsigned integer concepts above `MaxInt64` fail before dispatch. Under maps,
integer concepts must lie within ±2^53, including nested collections and pointers.
These checks inspect the codec's actual scalar JSON, so a struct wrapper cannot
bypass kernel limits. Concept map keys and arbitrary custom schema codecs are
not supported by this slice.

## Binary values

Use `[]byte` for binary content. No codec declaration is needed. Named slice types
whose element is the built-in `byte` have the same representation, provided they
have no custom JSON/text marshaler. Fixed `[N]byte` arrays remain integer arrays.

| Declaration/value | JSON and schema |
| --- | --- |
| `[]byte{1}` | `"AQ=="`; string with `byte-array` format |
| Empty non-nil `[]byte{}` | `""`, not `[]` |
| Nil byte-slice property | Omitted |
| `*[]byte` | String/null schema with `byte-array?` format; nil pointer omitted |
| `[][]byte` and other collections containing binary | Refused before registration: the pinned kernel's projection conversion loses binary elements |

Direct and nested object properties share this representation. Missing or null
non-pointer binary properties decode as owned, non-nil empty slices, including
on event reads. Nullable pointers remain nil; present strings decode to fresh
storage. Absent embedded pointers stay absent: an empty binary default never
creates an optional object. Field metadata classifies binary as
`serialization.Binary`, not `String` or a byte collection. `IsPrimitive` excludes
binary, and `ContainsBinary` also identifies objects owning binary descendants.

Reads require padded standard base64 with zero padding bits. URL-safe and
unpadded encodings, invalid tokens and whitespace inside the string return
`chronicle.ErrProtocol` without exposing the payload. The packaged C# client
accepts the captured leading-space/trailing-newline forms and null binary array
elements; Go deliberately narrows these reads. Both clients write canonical
base64, including System.Text.Json's `\u002B` escape for `+`.

Binary under collections, maps, derived variants, concepts, protection, indexes,
unique constraints or subject declarations is refused before registration. This
includes `chronicle:"unique"`, `UniqueValues(...).On(...)`, `chronicle:"subject"`,
`WithSubjectProperty`, `chronicle:"index"` and `WithIndexes`. Unique constraints
and indexes also refuse objects containing binary. The pinned kernel hashes a
byte array's `ToString()` value, not its contents; subject identities likewise
lack a qualified cross-client representation. Admission stops at direct/nested
object properties and one pointer to a binary leaf. The pinned kernel preserves
binary arrays in event history but its projection converter treats each byte
array as another collection; read models lose their byte values. See the
[binary kernel limitation](parity.md#baselines-and-evidence) for source citations
and [Chronicle#4595](https://github.com/Cratis/Chronicle/issues/4595) for the upstream fix.
Go does not repair these lossy read models or weaken its base64 decoder. AutoMap
requires the same compiled binary representation and ASCII property names;
CLR Unicode case matching is not qualified. Explicit mappings, binary-to-string
conversion, initial values, literals, arithmetic, identities, keys, joins and
runtime ordinary-scalar profiles are unsupported. Read-model identity and subject
fallback properties cannot be binary, even without a projection. This includes
all case variants of serialized `id` and the MongoDB `_id` property. A dotted
JSON name cannot hide binary from path guards: if any literal-name or nested-path
candidate contains binary, indexes, uniqueness, subjects, keys, explicit writes
and single initial values are refused, including after naming rebinds. Whole-model initial values inspect actual
root ownership, including properties whose JSON names contain dots.

Event migrations involving either binary-containing endpoint are refused by
`DefineMigration` and catalog `WithMigrations` before I/O. This includes nullable
and nested binary fields, identity migrations, rename, default, split, combine
and directional/shared value maps. The pinned kernel's migration operations
transform JSON strings without binary-aware validation; splitting `"AQ=="` on
`"="` produces invalid base64. No migration operation has binary qualification.
Keep binary-bearing generations without migrations until such a path is qualified.
Snapshot naming rebinds retain the binary representation and do not decode or
reinterpret bytes.

The [packaged binary capture](../serialization/testdata/binary/README.md) records
Chronicle 19.29.4, Fundamentals 7.19.6 and both schema generator APIs. Class
properties in C# have no required list; its record control requires `Payload`.
Go retains its existing field-required rules: slice and pointer properties are
not required, while ordinary value-struct containers can be required. Binary
admission does not overwrite persisted schemas: enable generation validation
and plan a new generation when a historical representation differs.

## Shared correlation context

`metadata.WithCorrelation` and `metadata.Correlation` share Fundamentals'
`correlation.WithID` / `FromContext` key. A correlation set by Arc.Go reaches
Chronicle appends without application glue, and Chronicle-set IDs are visible to
Arc. Explicit zero shadows a parent value; reads never generate an ID.
`metadata.CorrelationID`, its broader parser and existing text/JSON codecs remain
unchanged. Identity, causation, event-source IDs and tenancy remain Chronicle-owned.
