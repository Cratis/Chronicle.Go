# Packaged C# naming capture

`profile.json` is output from `capture.cs`, not a hand-authored naming oracle.
The Go comparison is `serialization/naming_capture_test.go`. Regeneration is
manual; `go test` does not restore or execute .NET packages.

## Provenance and regeneration

- Package: `Cratis.Chronicle` **19.153.0**, the published client matching the
  **19.153.0-development** kernel line in `.github/workflows/integration.yml`.
- Direct naming-policy dependency: `Cratis.Fundamentals` **7.33.1**.
- SDK: **11.0.100**; runtime: **11.0.0**.
- Chronicle source revision recorded by the assembly:
  `9157c14098688ac0c6946a335bfabf1f44220c7c`.
- Fundamentals assembly informational version:
  `1.0.0+6ae7eb7d133afdb7adaf161ca185eaa189c7372b`. This is distinct from the
  pinned NuGet package version, **7.33.1**.
- Generation date: **2026-11-21**, UTC.

From the repository root, with that SDK installed and a writable `.ai-work/`
directory, run:

```sh
dotnet run --file serialization/testdata/naming/capture.cs -- .ai-work/naming-profile.json
```

The output path must not already exist. Review the new output before replacing
`serialization/testdata/naming/profile.json`; do not edit names in the capture to
make a Go test pass. This file-based app pins both direct package references and
uses no Chronicle server. It initializes the packaged client's actual JSON
options without starting discovery or a connection. Empty artifact, compliance
and derived-type providers isolate this naming witness; any event-registry access
fails the capture. No event registration or kernel operation is performed.

## What is compared

Each of `DefaultNamingPolicy` and `CamelCaseNamingPolicy` captures three types:

- `NamingEvent`: `URLValue`, `ID`, `Person` and nested acronym/ordinary properties.
- `NamingReadModel`: the same shape with the conventional C# `Id` identity.
- `ExplicitEvent`: explicit `JsonPropertyName` overrides, an `Id` member fixed to
  the shared spelling `Id`, and renamed nested objects and leaves.

For each type the app calls the packaged `EventSerializer.Serialize`,
`JsonSchemaGenerator.Generate` and `GenerateForReadModel`. Field names and nested
serialized paths come from the client's `JsonTypeInfo`; source and target paths
come from the packaged projection `ProjectionPropertyPaths.FromExpression`
helper, invoked reflectively because it is internal. These are helper outputs,
not a complete projection registration or projection execution capture.

Go specs compare decoded payloads, schema **property names** and compiled field
names/paths: event plans against `NamingEvent`, read-model plans against
`NamingReadModel`, and both plans against `ExplicitEvent`. C# `Id` maps to Go
`ID` through the root read-model convention or the explicit shared `Id` spelling.
The schema comparison intentionally does not compare required lists, nullability,
formats, titles or other schema keywords.

## Reflection-path difference

C# serialization and schema generation honor `JsonPropertyName`, but this
packaged projection expression helper applies only the naming policy to CLR
member names. For example, `ExplicitEvent.Nested.Person` serializes at
`details.display_name`, while its source and target reflection paths are
`Nested.Person` by default and `nested.person` with camelCase. Likewise, explicit
`Id` stays `Id` in the payload and schema but becomes `id` in the camelCase
reflection paths. The tests retain and assert these differences. Go field and
projection-path metadata intentionally follow explicit `json` tags throughout.

## Untagged read-model identity difference

The capture also exposes an intentional Go identity convention, not universal
spelling parity: `CompileReadModel` translates an untagged root Go `ID` to `Id`
before applying the naming policy. Consequently that read-model field emits
`Id` by default and `id` with camelCase, while a C# property declared `ID` stays
`ID` under both policies. Event plans preserve `ID` on both sides.
`TestUntaggedReadModelIDMatchesDocumentedDifference` separately asserts the C#
`ID` and Go `Id`/`id` payload/schema keys and Go identity metadata, referencing
the convention documented on `CompileReadModel` in `serialization/plan.go`.
It fails if either side changes; it does not normalize the capture. The ordinary
read-model equality spec uses `NamingReadModel.Id`. A shared explicit `Id`
spelling also avoids the difference, as shown by `ExplicitEvent`.

This witness covers naming only. It does not establish full schema
compatibility, same-generation registration compatibility, persisted history
compatibility or cross-client kernel interoperability. Both producer orders and
generation-validation checks against a real kernel require separate integration
coverage.
