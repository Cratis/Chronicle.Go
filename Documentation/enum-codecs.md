---
title: Declare Int32 enum codecs
description: Keep your named Go enums while sharing declared numeric values and read-model names with Chronicle.
---

Use this **partial, opt-in profile** when a C# Int32 enum must keep the same
numeric values and member names in Go. You own the named type and constants;
Chronicle owns no generic enum value wrapper and discovers no constants.
Unregistered named integers retain their ordinary integer behavior.

## Declare the member table

This declaration uses the same API as the executable
[`ExampleEnum`](../serialization/example_enum_test.go):

```go
type ApprovalStatus int32

const (
    Pending ApprovalStatus = 0
    Approved ApprovalStatus = 1
)

type ApprovalChanged struct {
    Status ApprovalStatus
}
```

Pass the immutable codec set to your existing event or read-model declaration.
This excerpt assumes the types above and imports `serialization` and `events`
from `github.com/cratis/chronicle.go`:

```go
codecs, err := serialization.NewCodecs(serialization.Enum(
    serialization.EnumMember[ApprovalStatus]{Name: "Pending", Value: Pending},
    serialization.EnumMember[ApprovalStatus]{Name: "Approved", Value: Approved},
))
if err != nil {
    return err
}
event, err := events.Define[ApprovalChanged](events.WithCodecs(codecs))
if err != nil {
    return err
}
data, err := event.Descriptor().Marshal(ApprovalChanged{Status: Approved})
```

The payload is `{"Status":1}`. Decoding `{"Status":"approved"}` returns
`Approved`. `readmodels.WithCodecs`, `serialization.CompileWith` and
`CompileReadModelWith` use the same set. Property naming policies and explicit
`json` names still apply; enum member names never undergo property-name casing.

Use `serialization.Flags` instead of `Enum` for a flags declaration. Declare
composites explicitly, such as `AB=3`. `"A, B"` is accepted only if its bitwise
result is a declared value. Declaring `All=-1` admits exactly `-1`, not every
possible combination. No zero, aliases or combinations are manufactured.

## Admitted shape and validation

| Boundary | Contract |
| --- | --- |
| Backing type | Caller-defined named `~int32`; built-in `int32` and aliases to it fail construction. Other backings do not satisfy the generic constraint |
| Members | Nonempty table; names contain only ASCII letters (`[A-Za-z]+`). Duplicate values, aliases and case-insensitive name collisions fail `NewCodecs` |
| Identity | Exact Go type, enum/flags marker and full member table; one registration per type |
| Placement | Direct, unprotected ordinary DTO root properties `T`, `*T`, `[]T`, and their `Field.Marshal` codecs |
| Writes | Exact numeric Int32 primitives, declared values only; zero and negative values must be declared |
| Reads | Integer JSON tokens, case-insensitive names, numeric strings, surrounding ASCII spaces and comma-separated names whose final value is declared |
| Invalid reads | Null nonnullable scalar, undeclared value, null array element, fraction, exponent, overflow, boolean, object or malformed name fails the entire decode |
| Missing scalar | Zero only when zero is declared; otherwise decoding fails rather than returning an undefined Go value |
| Nullable scalar | Missing/null remains nil; nil object properties are omitted on write, while `Field.Marshal` preserves explicit null |
| Arrays | Missing/null input normalizes to an empty slice, in both event and read-model plans. Independently supplied nil slices are omitted on write |
| Application hooks | No `String`, JSON/text marshal/unmarshal, `IsZero` or constructor calls for enum values |
| Ownership | Members copied at declaration and `NewCodecs`; plans and naming snapshots retain frozen tables; failed decoding never publishes a partial target |

Numeric strings accept the captured decimal syntax, including `"+1"`, `"01"`
and `"-0"`. Comma lists contain names, not numbers. This profile does not extend
whitespace acceptance to arbitrary Unicode or other `Enum.TryParse` syntax.

Enum concepts, map keys/values, nested objects or collections containing enums,
nullable array elements, pointer-to-array properties, fixed arrays, promoted
embedded enum fields, omission tags, derived variants and protected enum
placements are not admitted. Tags, providers and explicit classifications all
reject protection rather than stripping metadata.

Schemas contain integer `enum` and aligned `x-enumNames`, ordered by CLR unsigned
Int32 magnitude. There is **no `format: int32`** on enum properties: that format
selects the kernel's ordinary integer conversion instead of enum-name conversion.
Nullable properties preserve the captured integer/null type array, with no added
null enum member. Enum scalar, nullable and array properties retain the captured
`required` entries. Ordinary non-enum integer formats and range guards are unchanged.

## Projections and historical generations

Copies, including AutoMap, require the same compiled enum profile, table, declared
type and nullable/array shape on both sides. Integer-to-enum, enum-to-integer and
cross-enum `MapAs` conversions are refused. Constants are validated through the
actual field codec; numeric model-bound literals cannot bypass membership.
Arithmetic, context-to-enum assignment, all-event enum mappings and enum
correlation keys are unsupported, including fluent `VariantKey` and generated
variant joins. Validation runs against the final graph, after shared handlers and
`EntersOn` create-or-update handlers are added.

AutoMap uses the kernel's case-insensitive property matching: event `status` can
populate model `Status` only with the same enum profile. Enum-active AutoMap
handlers require ASCII property names; ambiguous case-insensitive names and
Unicode names fail locally because CLR Unicode comparison is not qualified.
The rendered source name must also be a safe event-property expression:
`true`, `True`, `false` and `False` resolve as kernel constants, not event values.
Enum-involved automatic mappings using these names fail locally, including after
naming rebinding. Excluded properties and explicit target writes can suppress
AutoMap; explicit enum copies still require safe source expressions. Sparse
`EveryMap` checks expression safety even when the subscribed event lacks the
source property. Naming rebinding can use an unsubscribed catalog event, so the
check runs again on the final expression. Safe missing property paths remain
allowed and do not add subscriptions.

Direct root JSON names such as `json:"State.Value"` are admitted by serialization,
but cannot be used in enum-involved projection mappings: the kernel treats the
dot as a path separator, not part of the property name. These mappings fail
locally, whether the enum is on the source or target, including explicit
literal/null targets. Rename the JSON property to a simple name for projection
use. The guard uses structural field ownership; genuine nested ordinary paths
and ordinary non-enum mapping behavior are unchanged. This does not admit nested
enum placements or provide escaping for arbitrary JSON names.

For enum validation, encoded collection `Inherit` enables automatic mapping in
the pinned kernel even beneath a `NoAutoMap` root. Nested `Inherit` uses the
containing root or collection projection's mode, not the immediately enclosing
nested object's override. Set `NoAutoMap` explicitly on a collection child when
you need it disabled. These checks apply even when only the event contains an
enum; they do not admit nested enum model placements or qualify every C# node
inheritance behavior. Existing encoded modes and ordinary non-enum mappings are
unchanged.

Use `NoAutoMap` and explicit mappings when automatic matching is unsuitable.
Literal targets are resolved and type-checked before any serialization hook runs.
`Clear` can assign null to an ordinary nullable enum scalar.

Historical descriptors retain their own tables. A naming rebind cannot change
member values, names or the enum/flags marker, even for omitted or null fields.
Migrations with a declared enum at either endpoint currently return
`ErrUnsupported`, including identity, default, copy and value-map migrations.
This is an explicit boundary, not an unchecked generic JSON escape hatch.

Before adopting this profile for existing data, inventory every stored number and
combination. Adding a codec changes an ordinary integer schema to a closed enum
schema; do not silently replace a persisted generation. Keep old descriptors and
coordinate schema evolution with a client that supports the required migration.
Do not remove/renumber members, rewrite historical bytes or use a different table
merely to make decoding pass. A table without zero requires a real scalar value
in every decoded document; the SDK does not invoke C# constructor defaults.

## Compatibility evidence and deliberate differences

The owning profile is Chronicle **19.29.4**, Fundamentals **7.19.6**, and
.NET/System.Text.Json **10.0.12**. The immutable
[package capture](../serialization/testdata/enum/README.md) executes the actual
client options initializer, `EventSerializer`, both schema APIs and in-memory
Expando conversion. It is not interchangeable with the historical 236-case
Fundamentals source corpus or the newer `e8ac1ec` mask fix.

C# can write unknown values and parse undeclared numeric strings or combinations.
Go deliberately refuses them: Chronicle's schema conversion can turn unknown
scalars into the default name, drop unknown nullable scalars, or turn unknown
array elements into null. The newer Fundamentals flags mask does not fix that
Chronicle mapping boundary. This is not a claim that all C# operations reject
unknown values, and it is not a default string-enum serializer.

`TestEnumPackagedProfileRegression` covers all scalar/nullable/array observations
for the five admitted Int32 declarations under both policies. The supplemental
[typed .NET decoder](../serialization/testdata/enum-roundtrip/README.md) checks
Go payloads without modifying the raw capture. `TestKernelDeclaredInt32EnumProfile`
checks numeric event history and named Mongo-backed model reads from both captured
C# and Go writes, including signed limits, declared composites, `All=-1`, arrays,
nullable values and updates to zero. Its exported event/model JSON also goes
through the actual typed .NET decoder. These tests do not qualify other backing
types, protected enums or enum migrations.
