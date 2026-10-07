# Captured C# derived children definitions

These goldens were produced by running the packaged C# client, Chronicle
**19.32.3** with Fundamentals **7.22.0** on .NET **10.0.12**, through the
[capture harness](capture/Program.cs). They are never written by hand or by the
Go encoder. The [provenance](provenance.json) pins the packages, source revisions,
runtime and SHA-256 hashes; `TestDerivedChildrenCaptureProvenance` checks them.

## What is captured

For each naming policy the harness builds the model-bound definition of
`Catalog`, whose `Items` collection holds the `Child` interface with exactly one
`[DerivedType("line")]` implementation, `Line`. It also serializes and decodes a
`Catalog` with one `Line` through `DerivedTypeJsonConverterFactory`.

- `*.definition.json` shows `ResolveConcreteChildType` selecting `Line` for the
  identity, key and parent key, and `AddDerivedTypeDiscriminatorMapping` writing
  `_derivedTypeId: $value(line)` on the `ItemAdded` creator only. The join and
  removal carry no discriminator.
- `*.child.json` is the typed read-model payload. The derived converter writes the
  derivative's properties in camelCase under both policies.

The packaged builder is internal, so the harness constructs it by reflection with
a proxy `IEventTypes` that names each event after its CLR type. It normalizes the
contract objects only as a JSON carrier: event-keyed dictionaries become
key/value arrays and string-keyed dictionaries are sorted.

## Reproduce

SDK 10.0.401 and runtime 10.0.12 must be installed. From `capture/`:

```sh
dotnet restore DerivedChildrenCapture.csproj --locked-mode
dotnet run --project DerivedChildrenCapture.csproj --no-restore -- ../recaptured
```

The output directory must not exist. Compare it with the committed files rather
than overwriting them. Build outputs are locally ignored.
