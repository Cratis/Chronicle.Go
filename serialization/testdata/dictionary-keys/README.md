# Packaged C# dictionary-key capture

`profile.json` is output from `capture.cs`, not a hand-authored oracle. The Go
validator is `serialization/dictionary_key_fixture_test.go`
(`TestDictionaryKeyPackagedProfile`). Regeneration is manual; `go test` does not
restore or execute .NET packages. This is evidence for Cratis/Chronicle.Go#64
only. It implements and approves no Go dictionary admission.

## Provenance and regeneration

- Packages: `Cratis.Chronicle` **19.153.0** (source
  `9157c14098688ac0c6946a335bfabf1f44220c7c`) and `Cratis.Fundamentals`
  **7.33.1** (source `6ae7eb7d133afdb7adaf161ca185eaa189c7372b`).
- SDK **11.0.100**, runtime **11.0.0**. Hashes and the command are in
  `provenance.json`.

```sh
dotnet run --file serialization/testdata/dictionary-keys/capture.cs --no-restore -- .ai-work/dictionary-profile.json
```

The output path must not exist. Review before replacing `profile.json`, then
update `provenance.json`. The packages must already be in the local NuGet cache
(`--no-restore`); the capture execution itself uses no server, container or
network. This does not describe how the packages were obtained.

## Options paths

- **Serialize/Deserialize** use the configured client options produced by the
  packaged `ChronicleClient.InitializeJsonSerializationOptions`, run on an
  uninitialized client, once per naming policy (default and camelCase).
- **Schemas** come from `JsonSchemaGenerator.Generate(Type)` and
  `GenerateForReadModel(Type)`; both are captured separately.
- **Expando** directions use the one-argument `ExpandoObjectConverter`
  constructor, whose options are `Globals.JsonSerializerOptions`
  (`Cratis.Json.Globals` from Fundamentals; `Source/Infrastructure/Json/ExpandoObjectConverter.cs:26,40,46`). They are not
  the client options, and the .NET client does not use this converter. The
  fixture therefore does not claim both paths share one policy.

## Contents

Seven declared types (string, Guid, Int32 enum with an alias, string concept,
two-field record, nested dictionary, nullable values) are each observed under
both policies. Each input runs the stages `serialize` (value inputs only),
`parse`, `deserialize`, `lookup` (independently constructed keys and key count),
`rewrite` and, per schema API, `expando-read` and `expando-write`. A stage whose
dependency failed is recorded as `not-attempted`. Wire text, including raw
duplicate and colliding spellings, is kept as strings. Failures record only the
exception type chain.

Failures and surprises are observations, not defects to patch. For example, enum
keys fail to serialize and deserialize in this harness, and two spellings of one
Guid or record key collapse to one entry on read.

This witness is not Mongo/BSON, protection, older-server or cross-client kernel
proof.
