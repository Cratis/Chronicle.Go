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

## Shared correlation context

`metadata.WithCorrelation` and `metadata.Correlation` share Fundamentals'
`correlation.WithID` / `FromContext` key. A correlation set by Arc.Go reaches
Chronicle appends without application glue, and Chronicle-set IDs are visible to
Arc. Explicit zero shadows a parent value; reads never generate an ID.
`metadata.CorrelationID`, its broader parser and existing text/JSON codecs remain
unchanged. Identity, causation, event-source IDs and tenancy remain Chronicle-owned.
