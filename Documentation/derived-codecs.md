---
title: Register unprotected derived types
description: Serialize explicitly registered interface families with stable Chronicle discriminators and fail-closed validation.
---

Use derived codecs when an event or read model contains an interface with a
closed set of concrete representations. This is **partial C# compatibility for
unprotected data**, not support for protected polymorphism. Projections can
build children of a family with one registered derivative; see
[project derived children](#project-derived-children).

## Declare an isolated family

Define the interface and its implementations, then give each concrete type a
stable discriminator. Register the same codec set on every event and model that
uses the family. No global registry, type scanning or registration callback runs.

This excerpt uses the `Actor`, `Human`, `Robot`, `ActorsChanged` and `ActorView`
types from the executable [example](../serialization/example_derived_test.go).

```go
codecs, err := serialization.NewCodecs(
    serialization.Derived[Actor, *Human]("human"),
    serialization.Derived[Actor, Robot]("robot"),
)
if err != nil {
    return err
}
event, err := events.Define[ActorsChanged](events.WithCodecs(codecs))
if err != nil {
    return err
}
model, err := readmodels.Define[ActorView](readmodels.WithCodecs(codecs))
```

Handle the final error before using `model`. The equivalent options work with
`chronicle.RegisterEvent` and `chronicle.RegisterReadModel`.

`Family` must be an interface; `Concrete` must be an assignable named struct or
pointer to one with an ordinary object codec. Selection uses the **exact** dynamic
Go type. Registering `*Human` does not admit `Human`. Duplicate registrations,
conflicting IDs, pointer/value duplicates and discriminator/property collisions
fail. IDs are unique across the set; a concrete type explicitly registered for
multiple families must retain the same ID. Options are last-wins and capture the
set when created; nil clears the option.

For lower-level tooling, use
`serialization.CompileWith(typ, serialization.Config{Codecs: codecs})` or
`CompileReadModelWith`. The old compilation APIs still admit no interfaces.

The example emits:

```json
{"Actors":[{"name":"Ada","_derivedTypeId":"human"},{"model":"R2","_derivedTypeId":"robot"}]}
```

The discriminator follows the derivative's fields in declaration order, with the
same default System.Text.Json escaping as ordinary payload strings. The wire
schema is an **open object**, matching Chronicle's generator, not an invented
`oneOf` schema. The plan separately retains the closed admitted variant graph for
validation, decoding and metadata.

## Keep names and generations stable

A derivative's immediate properties use Fundamentals' acronym-preserving
camelCase, independently of the client policy: `Name` becomes `name`, while
`URLValue` stays `URLValue`. Nested ordinary objects use the configured policy.
The same concrete Go type used outside a family keeps ordinary naming and has no
discriminator.

Explicit Go `json` tags still win. This deliberately differs from the pinned C#
derived converter: `[JsonPropertyName("explicit_label")]` on `Label` is ignored
when writing through a family, producing `label`. Go's `json:"explicit_label"`
produces `explicit_label`. The captured .NET fixture demonstrates this difference;
**matching attribute/tag text does not establish cross-client wire agreement**.
Choose Go tags to match the actual shared wire names.

Names, codecs and resolved classification-provider results remain frozen through
client composition and reconnect. Snapshot renaming follows concrete identity
and discriminator without calling codecs or `IsZero`. A changed discriminator or
concrete representation is not a naming change. Historical event declarations
need their own original `WithCodecs` configuration; newer registrations do not
rewrite earlier generation plans. Variant fields are not ordinary migration
paths. Plan a schema/content migration separately from a naming-policy change.

## Avoid the direct-child C# converter defect

The pinned Fundamentals converter loses the declared family context of a
derivative's **direct interface-valued property**. It writes that child without
`_derivedTypeId`; its own decoder then returns null for a non-null input.
[Fundamentals #1149](https://github.com/Cratis/Fundamentals/issues/1149) tracks the
captured failure with Chronicle 19.29.4 / Fundamentals 7.19.6.

Go rejects this declaration before I/O, including aliases, pointer layers and
promoted fields. Every registered variant is checked, even if it is unused by the
root or absent from a sample. Go neither manufactures an undocumented stronger
wire representation nor silently decodes a missing discriminator as null.

Declared family collections retain their element context. Arrays/slices and
string-keyed maps round-trip in the captured .NET fixtures, including recursive
family arrays. An ordinary nested object retains its own configured field plan.
Consider a collection only when it represents the domain correctly: replacing a
single child with a one-element array is **not** an equivalent business model.

## Failure and security boundaries

- Unknown implementations never fall back to ordinary JSON. Missing, unknown or
  nonstring `_derivedTypeId` members fail decoding. Ordinary variant property reads
  remain case-insensitive; the discriminator requires its exact spelling. Variant
  declarations cannot use any case-folded spelling of this reserved property,
  including promoted fields and fields tagged `omitzero`.
- Decoding and snapshot rebinding reject duplicate JSON member names throughout
  the input, including maps, arrays and unknown properties, before calling codecs
  or normalizing read-model IDs. Names are compared exactly after resolving JSON
  escapes; ordinary names differing only by case are not duplicates. Structural
  validation is bounded to 100 MiB and 256 nesting levels.
- Nil interface properties are omitted. Typed-nil variants and null family
  collection elements fail. C#'s derivative dictionary can retain null entries
  despite `WhenWritingNull`; Go intentionally keeps its existing omission policy.
- Read-model collection normalization follows the selected compiled variant;
  it does not inspect concept/codec internals. Failed decoding publishes no partial
  target, model or decision token.
- The kernel converts numbers inside open objects through doubles. Every integer
  in an admitted family subtree must be within ±2^53. The existing unsigned
  `MaxInt64` append guard and string-keyed-map restrictions also remain.
- Application codec, metadata-provider and `IsZero` failures have payload-free
  messages. Ordinary causes remain available through `errors.Is/As`; deliberate
  cause inspection can expose sensitive data or invoke application error methods.
  Decision reads contain panics from `As`/`Unwrap` classification as well as codec
  execution, returning no model or token. Panic values are discarded.
- Classified families and variants are rejected, including type/provider metadata
  on unused variants. A document containing an open family beside protected
  siblings is also rejected: the kernel's active protection walk cannot safely
  traverse that open branch. Metadata is never deleted or silently promoted to
  collection protection. Existing [protection placement limits](compliance.md)
  remain in force.

`*serialization.CodecError` identifies invalid family declarations with declared
family/concrete types, the Go field and discriminator property name; it does not
print discriminator values. It matches `chronicle.ErrInvalidConfiguration`.
Decode errors match `chronicle.ErrProtocol` and preserve ordinary typed causes.

## Project derived children

A projection builds each child of a collection from an event, so the kernel, not
your code, writes the child document. For an interface collection that document
also needs `_derivedTypeId`, or the family codec cannot read it back. When the
family has exactly one registered derivative, `children` resolves that concrete
type and writes its discriminator on the creating event, as the C# client does
for a `[ChildrenFrom]` collection with one `[DerivedType]` implementation. These
declarations come from the executable
[example](../projections/example_derived_children_test.go):

```go
type Parcel interface{ parcel() }

type Box struct {
    BoxID  string `json:"boxId" chronicle:"key"`
    Weight int    `chronicle:"set(BoxPacked,from=Weight)"`
}

func (*Box) parcel() {}

type Shipment struct {
    ID      string   `json:"id" chronicle:"key"`
    Parcels []Parcel `chronicle:"children(BoxPacked,key=BoxID,parent-key=ShipmentID)"`
}

codecs, err := serialization.NewCodecs(serialization.Derived[Parcel, *Box]("box"))
model, err := readmodels.Define[Shipment](readmodels.WithCodecs(codecs))
```

The child node uses `Box`'s fields: its `key` field is the child identity and its
mapping tags apply to the child, in the derivative's camelCase names. Every
child `From` gains `_derivedTypeId: $value(box)`, not only the creating event.
The kernel adds a child for any non-join `From` whose identity is absent, so a
keyed update that arrives after the child was removed, or before it was created,
writes a new child; the discriminator keeps that child readable. On an existing
child the update rewrites the same constant. Joins never add a child, so they
carry no discriminator, and removals delete the child. The C# client stamps only
the `[ChildrenFrom]` creator, so a recreated C# child has no discriminator. The
fluent `projections.Children` accepts the same family with `Builder[Box]` and
stamps every child `From` the same way.

Compilation fails rather than writing undecodable children when:

- the family has no or several registered derivatives (C# silently keeps the
  interface and writes no discriminator),
- the discriminator is not representable as a kernel literal,
- a global (`every`/`all`) mapping targets any casing of `_derivedTypeId`,
- a derivative declares mappings but its family is not a `children` collection,
  including `nested` interface fields, which C# does not resolve either.

A derivative's `key`, `no-auto` and `not-projected` tags are type metadata, not
mappings. A fluent derivative needs its `key` tag, and another read model may
hold the same family as a whole property: that model is not discovered as a
projection by those tags alone, and compiling it does not refuse them.

Whole-family properties or collections can still be copied when source and
target compiled representations agree. Projections with children are
relationship projections, so `ReadModels.ReplayProjection` refuses them; replay
the observer instead. Protected families stay refused (see
[#64](https://github.com/Cratis/Chronicle.Go/issues/64)).

`Field.Derivatives()` exposes detached, variant-qualified fields for future
compilers. Variant properties are never flattened into ambiguous `FieldAt` paths,
and `_derivedTypeId` is not a writable Go field. `Field.Type` keeps declared domain
identity rather than replacing it with its underlying scalar type.

Enums, binary/GeoJSON representations, custom schema codecs, complex map keys and
protected polymorphism remain separate work under
[#64](https://github.com/Cratis/Chronicle.Go/issues/64). See the
[parity map](parity.md#registered-derived-codecs) for the exact evidence and limits.
