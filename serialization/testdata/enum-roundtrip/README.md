# Typed enum round trips through the owning packages

This supplemental program consumes Go-produced payloads or actual kernel
readbacks. It executes the packaged `EventSerializer.Deserialize` into the exact
captured scalar, nullable or array CLR type, compares typed numeric identity, then
serializes and deserializes once more. It fails on an empty case set, mismatch,
serializer failure or forbidden registry access.

The historical capture remains immutable. This project links its unchanged
`Declarations.cs` and `Isolation.cs`, uses its locked packages, and has no copied
converter implementation. It does not claim full CLR enum behavior or change the
historical source corpus. The product subset is described in
[declared Int32 codecs](../../../Documentation/enum-codecs.md).

## Run locally

Prerequisites are the already-installed SDK **10.0.401**, runtime **10.0.12**,
and cached Chronicle **19.29.4** / Fundamentals **7.19.6** locked packages. Run
restore and build from `enum/capture` so its pinned `global.json` selects the SDK.
The runtime cannot roll forward. Missing cached dependencies fail the offline
restore; nothing installs a runtime or changes root Go dependencies.

From the repository root, with task-owned ignored output paths:

```sh
root="$PWD"
mkdir -p "$root/.ai-work/tasks/enum-codecs/cache/empty-feed" \
  "$root/.ai-work/tasks/enum-codecs/keep"
cd serialization/testdata/enum/capture
dotnet restore ../../enum-roundtrip/EnumRoundtrip.csproj --locked-mode \
  --source "$root/.ai-work/tasks/enum-codecs/cache/empty-feed" \
  -p:BaseIntermediateOutputPath="$root/.ai-work/tasks/enum-codecs/output/obj/" \
  -p:OutputPath="$root/.ai-work/tasks/enum-codecs/output/bin/"
dotnet build ../../enum-roundtrip/EnumRoundtrip.csproj --no-restore -c Release \
  -p:TreatWarningsAsErrors=true \
  -p:BaseIntermediateOutputPath="$root/.ai-work/tasks/enum-codecs/output/obj/" \
  -p:OutputPath="$root/.ai-work/tasks/enum-codecs/output/bin/"
cd "$root"
CHRONICLE_ENUM_ROUNDTRIP_OUTPUT="$PWD/.ai-work/tasks/enum-codecs/keep/go.json" \
  go test -count=1 ./serialization -run '^TestEnumPackagedProfileRegression$'
dotnet .ai-work/tasks/enum-codecs/output/bin/EnumRoundtrip.dll \
  .ai-work/tasks/enum-codecs/keep/go.json
```

Export files must not already exist. Use a new filename instead of overwriting
retained results. On hosts with execution-budget or work-artifact wrappers, run
each phase separately through those wrappers and register its workspace first.

## Check actual Mongo readbacks

Use a disposable `cratis/chronicle:19.29.4-development` instance, following the
repository's integration certificate and HTTPS readiness setup. Never point the
fixture at a production store. The test creates short random store names and
uses the `Default` namespace, separate event/model identifiers and both naming
policies. Capture and decode its raw outputs:

```sh
CHRONICLE_INTEGRATION_CONNECTION_STRING=chronicle://localhost:35198 \
CHRONICLE_ENUM_KERNEL_OUTPUT="$PWD/.ai-work/tasks/enum-codecs/keep/kernel.json" \
  go test -tags=integration -count=1 -timeout=2m ./internal/integration \
  -run '^TestKernelDeclaredInt32EnumProfile$'
dotnet .ai-work/tasks/enum-codecs/output/bin/EnumRoundtrip.dll \
  .ai-work/tasks/enum-codecs/keep/kernel.json
```

The profile produces **262** ordinary Go payload round trips and **96** kernel
readbacks: numeric event history and named read-model JSON from captured C# and
Go producers. The focused kernel profile was executed on image digest
`sha256:23d9fc83e1764e3fcfad2326e4404d8fd081008c3ebc8c46be059fed09e4c261`.
`All=-1`, declared `AB=3`, Int32 limits, empty arrays, nullable values and zero
updates survive typed readback. Unknown writes are refused locally. This does
not test protected enums, alternate backings, undeclared combinations or a
migration engine, and in-memory Expando observations remain a separate profile.
