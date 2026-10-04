# Actual-package authentication observations

`profile.json` contains 55 observations from installed **Cratis.Chronicle
19.29.4**, **Cratis.Fundamentals 7.19.6**, .NET **10.0.12**, Arm64. These are
client-side representation and request observations, not authentication tests.
All credentials, URLs, headers, event IDs and store names are fixed synthetic
inputs. Never replace them with application credentials.

## Boundaries

- Inbound: actual public `WebhookSourceBuilder.Build`, public borrowed-connection
  `ChronicleClient` construction and actual public `EventSerializer` construction
  and methods. Public-constructor reflection handles the activator type omitted
  from the reference assembly; the activator is unused because additional event
  information providers are explicitly empty. No private state is initialized or
  modified, and no converter initializer is copied.
- Common JSON options come from the constructed client's public `Options`.
  Preserve-name and Fundamentals camelCase policies are recorded separately.
  Direct `SourceAuthorization` serialization also executes the actual converter;
  this includes public OAuth converter serialization, **not** an OAuth Register
  factory. Both policies retain the converter's fixed lowercase inner keys.
- Outgoing: actual `Webhooks.Register` with fixed catalog/store/service
  collaborators. The real public
  `InProcessAwareGrpcClientProxiesClientFactory.CreateClient<T>(CallInvoker)`
  creates the proxy. On this package it returns a protobuf-net dynamic proxy;
  there is no proxy assembly file to hash. The owning factory, contracts and
  marshaller assemblies are hashed. No factory fields are assigned by the harness.
- The recording invoker captures the actual request object and invokes the
  supplied request marshaller in both directions. Its response is a synthetic
  local default response, **not** a server acknowledgment. Six Register calls
  produce six captured requests and six independent decoded objects.
- Empty type discovery is supplied through public `IServiceProviderIsService`
  and `ITypes`; it cannot fall back to application discovery. The artifacts list
  and derived-type universe are empty. No `GetCatalog` is needed on this package.
- The only connection operations permitted are the six local service-accessor
  reads and two local disposal calls. `GetEventStore`, `Connect`, HTTP, schema
  registration, live source catalogs and every unexpected collaborator operation
  are forbidden. A caught forbidden operation invalidates the **whole** capture.
  The final allowed-call inventory must match exactly; zero or missing permitted
  calls are not success.
- No encryption, credential persistence, authentication enforcement, token
  acquisition, delivery or live-kernel behavior is observed. Plain strings prove
  representation only. Causation metadata created internally by the public client
  constructor is never exported. Errors retain types only, not messages or stacks.

The C# source authority is `2e31b0dfba489159b3db323238f16d0f277056b4`.
The package's own source revision is separately recorded as
`ae5e00a8abaa688138b2c2f689e2b4659cccb4fd`; these are not interchangeable pins.
`provenance.json` freezes package/assembly/harness hashes, runtime, configuration,
call counts and the raw artifact hash. `case-inventory.json` is an independently
specified inventory, never inferred from whatever cases a capture happens to emit.

## Interpretation

Default inbound authorization is null (omitted); explicit None writes
`{"type":"none"}`. The outer source `Type` enum writes the number `1`, while the
inner authorization discriminator is a string. Missing, unknown and case-changed
inner discriminators read as None in this package. Missing credential properties
throw `KeyNotFoundException`. Each read records a separate reserialization result,
including an explicit not-attempted result when reading failed. None fallback is
**not** permission to silently downgrade authentication in a future Go decoder.

C# outgoing default-true flags are omitted by protobuf-net; Go writes explicit
true. Explicit false is present in both. Validators compare semantics and presence
separately, including nested request bytes. `oauth-unavailable` is mandatory:
the public outgoing builder exposes Basic/Bearer, not an OAuth Register method.

## Reproduce without a server

Use the already installed, cached packages and SDK 10.0.401. Do not install or
resolve newer dependencies to reproduce this profile. Create a task-owned empty
feed and output directory; substitute their paths below. Run build and execution
as separate bounded phases. `PACKAGE_CACHE` is the existing NuGet package cache;
`WORK` is a task-owned ignored workspace, not the fixture directory.

```sh
dotnet build captures/testdata/authentication/capture/AuthenticationCapture.csproj \
  -c Release -p:RestoreLockedMode=true --source "$WORK/empty-feed" \
  -p:BaseIntermediateOutputPath="$WORK/obj/" -o "$WORK/bin"

dotnet "$WORK/bin/AuthenticationCapture.dll" "$WORK/profile.json" \
  captures/testdata/authentication/capture "$PACKAGE_CACHE"
```

The output file must not exist. The capture returns nonzero for unexpected calls
or counts and prints only error categories and stable operation names. A failed
attempt is not a partial fixture to publish. The reference assemblies omit the
contracts assembly, so the project references the actual runtime contracts DLL
from the same installed package; it adds no package dependency.

Ordinary `go test ./captures ./webhooks` validates the frozen evidence without
.NET or a kernel. Mutation tests reject missing/duplicate cases, altered metadata,
wrong casing/types/discriminators/union arms, incomplete secondary results,
malformed or altered request bytes, forbidden calls and invented live or encryption
claims. They do not implement inbound Go authorization.
