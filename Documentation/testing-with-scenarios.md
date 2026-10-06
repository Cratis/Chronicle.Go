---
title: Testing with scenarios
description: Test events, read models and reactors through production Go client conventions, with explicit fidelity boundaries.
---

Use `github.com/cratis/chronicle.go/chronicletest` to arrange events and assert
read-model state or reactor results in ordinary Go tests. This experimental kit
uses the same registered-type discovery, serialization, fold plans and invoker as
the client. It needs neither a dependency-injection container nor testify.

Choose the **real kernel** when the assertion concerns kernel behavior. Unlike
C#'s in-process fixtures, the Go substitute cannot load and run the C# kernel.

| Scenario | Execution | What it does not prove |
| --- | --- | --- |
| `EventScenario`, `Kernel` | Production client registration, append and reads against a real server | Encryption support or multi-node behavior merely from a successful append |
| `EventScenario`, `Substitute` (zero-value engine) | Production client over a private in-memory append/read transport | Constraints, concurrency enforcement, hashing, schema migration/validation, encryption, authorization, persistence or observers |
| `ReadModelScenario[M]`, reducer | Production fold plan in-process, one scope per source | Kernel delivery, storage, retries, quarantine or compliance |
| `ReadModelScenario[M]`, projection (default) | Production compiler/encoder and real kernel bounded replay RPC | Materialized-sink catch-up, persistence or observer completion |
| `ReadModelScenario[M]`, `Materialized: true` | Observer completion evidence, then production reads of the kernel's real sink | Observer lifecycle conformance, delivery metadata or effect acceptance |
| `ReactorScenario[R]` | Production discovery, scope activation, middleware and invoker; seeded read models and recording effects | Transport acceptance, durable checkpoints, retries, quarantine or exactly-once handling |

## Arrange an event scenario

Register the same named event types your application registers. Registration is
explicit, isolated and snapshotted; there is no global assembly scan or alternate
test discovery convention. The following test body assumes `registry` contains
`AccountOpened` and `ctx` is your test context:

```go
scenario := chronicletest.NewEventScenario(t, chronicletest.Config{
    Registry: registry,
    Engine:   chronicletest.Substitute,
})
if err := scenario.Given(ctx, "account-1", AccountOpened{Name: "Ada"}); err != nil {
    t.Fatal(err)
}
history, err := scenario.EventLog().ReadSource(ctx, "account-1", eventsequences.SourceFilter{})
if err != nil {
    t.Fatal(err)
}
if len(history) != 1 {
    t.Fatalf("got %d events, want 1", len(history))
}
```

`New*` helpers accept `testing.TB`, call `t.Helper()` and register cleanup.
`Open*` constructors instead return `(scenario, error)` and require `Close`; use
these outside tests and benchmarks.
[Complete compiling examples](../chronicletest/example_test.go) include all domain
types, registrations and imports for each scenario kind.

`Given` appends in order using the production client. An error stops seeding;
earlier committed events remain. Exercise production code with `EventLog()` or
`Store`; there is no test-only append implementation on the client side.
The substitute supports single append, same-source `AppendMany`, finite source
and sequence reads, tail, next and has-events. Other RPCs fail explicitly.

The substitute installs the client's no-check concurrency strategy, like C#'s
`EventScenario`; explicitly requested checks are refused, never reported as
successful. Registries containing constraints, read models or live observers are
refused before registration. Use the dedicated read-model/reactor fixtures for
local tests of those plans, and a kernel event scenario for actual observers.

### Use the pinned kernel

Set `Engine: chronicletest.Kernel`, `ConnectionString` to your test server, and
`Development: true` only for its self-signed development certificate. Tests use
`cratis/chronicle:19.29.4-development`. The fixture does not start Docker or silently
fall back to a fake.

```go
scenario := chronicletest.NewEventScenario(t, chronicletest.Config{
    Registry:         registry,
    Engine:           chronicletest.Kernel,
    ConnectionString: os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING"),
    Development:      true,
})
chronicletest.RequireFidelity(t, scenario.Fidelity(), chronicletest.Constraints)
```

An omitted kernel endpoint skips with a clear message in `New*`; `Open*` returns
`ErrKernelUnavailable`. A configured endpoint that cannot connect, authenticate
or register **fails**, rather than hiding broken integration infrastructure as a
skip. Set a context deadline for operations. Each default fixture creates a unique
store; an explicitly supplied store must be dedicated to this test. Closing a
scenario joins owned resources but does not delete server data.

For kernel constraint tests, register `constraints` definitions exactly as in
production, append competing values and assert `result.Err()` and persisted
history. The fixture does not implement a uniqueness validator in Go.

## Assert a read model

Register your model and reducer type with `RegisterReadModel` and
`RegisterReducer`, or register a model-bound/fluent projection. The fixture selects
the compiled producer for `M`. This excerpt uses the `AccountReducer` from the
complete examples:

```go
scenario := chronicletest.NewReadModelScenario[Account](t,
    chronicletest.Config{Registry: registry})
if err := scenario.Given(ctx, "account-1",
    AccountOpened{Name: "Ada"}, AccountRenamed{Name: "Grace"}); err != nil {
    t.Fatal(err)
}
instance, err := scenario.Instance(ctx)
if err != nil {
    t.Fatal(err)
}
if !instance.Exists || instance.Value.Name != "Grace" {
    t.Fatalf("unexpected account: %+v", instance)
}
```

Reducer scenarios require `Engine: Substitute` (the default). Like C#'s
`ReadModelScenario`, they use the real fold invoker in-process over substituted
storage. `Engine: Kernel` returns `ErrFidelityUnavailable` for a reducer rather
than silently running locally. Use a kernel `EventScenario` to test append-time
constraints or live reducer delivery.

Reducer results preserve absence/deletion and fail without partial state on fold
errors. Unsubscribed events do not invoke folds. Distinct sources fold separately;
`Instance` returns `ErrAmbiguousInstance` if more than one result exists. Choose
`InstanceFor(ctx, key)` or `Instances(ctx)` instead. Reads replay the collected
history, so keep folds deterministic. Synthetic sequence numbers increase across
`Given` calls and retain gaps for unsubscribed events.

For a projection, use `Engine: Kernel`. `Given` appends to the definition's selected
sequence. Keep this registry free of other Go reactors/reducers: projection
scenarios reject them before connecting, rather than accidentally executing
unrelated side effects. Use a kernel `EventScenario` when live observers are the
subject of the test. Result reads call the kernel's bounded projection replay, not a second
Go projection engine and not a sleep-based observer wait. The real kernel resolves
joins and custom keys. With no seeded events, reads return empty results without
an RPC, even if you explicitly borrow a populated store. Replay counts above
`math.MaxInt32` are rejected rather than wrapping the kernel's signed limit.
`Instance` counts all returned roots before selecting one.
Keyed access requires the kernel response to include a nonempty string ID: declare
a root `ID` field or `chronicle:"key"` with a kernel-compatible `id`/`Id` JSON name.
If the schema drops the key, keyed access fails instead of guessing from seed order.

Pass `ReadModelOptions[M]{Projection: &declaration}` to replace a discovered
projection **or reducer** for this fixture. This uses `Registry.WithProjection`
to create a detached registry and then the ordinary production compiler. The
original registry is unchanged. Models with explicitly conflicting producer/sink
metadata still fail normal production validation. `Initial: &model` supports
reducer initial state; projection initial state is explicitly unsupported.

### Assert projection initial values from the kernel sink

Set `ReadModelOptions[M].Materialized` to `true` when your projection declares
`projections.WithInitialValues`. The default bounded replay path still refuses
nonempty projection initial values with `ErrFidelityUnavailable`: the pinned
kernel's bounded replay does not apply them.

```go
options := chronicletest.ReadModelOptions[MaterializedAccount]{
    Materialized:            true,
    StrictEventSubscription: true,
}
```

The [compiling materialized example](../chronicletest/example_materialized_test.go)
shows registration, a 30-second context and result selection. It requires a running
kernel; the example is not executed by ordinary unit tests. This is a Go-specific
scenario mode, not a port of C#'s in-process projection processor.

Use an isolated store/namespace and let the scenario own the entire selected
sequence from position zero. Every successful `Given` append must return the next
position in its own history; foreign history returns `ErrFidelityUnavailable` and
blocks subsequent result reads. An observer or returned instance beyond the
scenario tail returns `chronicle.ErrProtocol`.

Result access waits through the production observer-completion API with exact
seed type references and positions. Because the kernel can report completion
before an observer exists, the fixture additionally requires that projection
observer to exist, have handled the subscribed seed tail, and have no failed
partitions. It retries **processing evidence**, within your context budget, not
sink contents. Missing or incomplete evidence returns
`ErrMaterializationIncomplete`, with no partial map or present zero model.
Then it reads the real sink once through `ReadModels().GetAll(ctx, id, nil)`.
No seeded events means empty results without I/O. Completion evidence is not a
transactional sink watermark; this mode adds no durability or lifecycle guarantee
beyond those production APIs.

Only active, unprotected root source-key projections with a real sink are admitted.
Passive/NoSink models, joins, children/nested projection definitions, custom keys,
variants and mixed `All` plus explicit subscriptions are refused before connection.
Reducers and the Substitute engine return `ErrFidelityUnavailable`.
`Initial` remains unsupported: **the fixture never overlays or fakes initial
state**. Defaults come exclusively from the kernel's materializing pipeline.
`ReadModelStorage` and `ProjectionExecution` are not substituted in this mode;
`ObserverLifecycle`, `DeliveryMetadata` and `EffectAcceptance` remain unproven.

### Reject unrelated projection seeds

Opt in when seeding an unrelated registered event should reveal a test mistake:

```go
scenario := chronicletest.NewReadModelScenario[ProjectedAccount](t,
    kernelConfig, chronicletest.ReadModelOptions[ProjectedAccount]{
        StrictEventSubscription: true,
    })
if err := scenario.Given(ctx, "account-1", AccountOpened{Name: "Ada"}); err != nil {
    t.Fatal(err)
}
```

This excerpt assumes your registry contains the `ProjectedAccount` projection and
`AccountOpened`, and `kernelConfig` selects your isolated kernel store. The
[compiling strict-subscription example](../chronicletest/example_test.go) shows
registration and checks `errors.Is(err, chronicletest.ErrUnsubscribedEventSeeded)`.

The default is `false`: unrelated registered seeds are still appended and ignored
by projection processing. With strict mode, `Given` rejects an unsubscribed event
immediately after registered-type lookup, before its codecs, outgoing providers,
append or history mutation. The error identifies only the selected projection and
event type IDs. Unknown event types still return `chronicle.ErrNotRegistered`.
Reducers ignore the option and retain their existing unsubscribed-event filtering.

**Check every `Given` error.** C# raises its strict-subscription error during lazy
result processing; Go deliberately raises from its existing error-returning
`Given`, before remote append. Result reads never reintroduce a rejected seed.
Earlier successful seeds remain, even when a later item in the same call fails.
There is no batch rollback or promise that retrying a failed call is safe.

This first strict profile admits only known, unclassified root source-key
projections: final root `From`/`RemovedWith` membership by event type ID, regardless
of generation, or pure `All` membership for all registered types. Ordinary `Every`
mappings do not subscribe additional event types. Inline replacement uses its
selected compiled definition, not the original producer. Unsupported
relationships, children/nested definitions, custom keys, derivative-group wire
forms, variants (even those with ordinary-looking wire fields), protected models
or subscribed events, and mixed explicit-plus-`All` subscriptions fail before
connecting. Membership does not depend on projection initial values;
nonempty defaults require `Materialized: true`. Projection `Initial` is still
refused, and `SeedReadModel` is not a projected-state overlay.

Strict seed checking does not prove observer attachment, partition/correlation
routing, retries, durability or distributed completion. The pinned-kernel sibling
checks persisted history and the synchronous bounded replay result, not asynchronous
replay-job completion; [#60](https://github.com/Cratis/Chronicle.Go/issues/60)
remains an unknown completion outcome. Projection seeding still serializes before
production `Append` serializes again; this option does not change that pipeline.

`SeedReadModel(key, value)` on read-model and reactor scenarios supplies a separate
snapshot for `ReadModels()` dependency reads. It does not seed projected state or
write a sink. Reducer constructors can receive explicit collaborators through
`Config.Services` or plain constructor closures, just as in production.

## Assert reactor results

Register the reactor and its event/read-model dependencies normally, then seed the
dependency needed by the handler. This uses `WelcomeReactor` from the examples:

```go
scenario := chronicletest.NewReactorScenario[*WelcomeReactor](t,
    chronicletest.Config{Registry: registry})
if err := scenario.SeedReadModel("account-1", Account{Name: "Chronicle"}); err != nil {
    t.Fatal(err)
}
if err := scenario.Given(ctx, "account-1", AccountOpened{Name: "Ada"}); err != nil {
    t.Fatal(err)
}
chronicletest.ShouldHaveProduced[WelcomeRequested](t, scenario.Produced(),
    func(event WelcomeRequested) bool { return event.Message == "Welcome Ada to Chronicle" })
chronicletest.ShouldNotHaveProduced[AccountClosed](t, scenario.Produced())
```

Each `Given` opens a fresh production batch scope; `PerEvent` registrations retain
their per-event activation policy. Constructor/handler failures and cleanup errors
are returned, and handler panics become errors through the invoker. Synthetic
`reactors.Delivery` identities are monotonic across calls, including failures.
Unsubscribed registered events advance the synthetic sequence but do not activate
scopes, invoke handlers or run middleware. They are not a simulation of kernel
replay or retry identities.

The recorder flattens collections, `eventsequences.Entry` and
`EventsWithConcurrencyScopes` to their payloads. It records returned commands
without running their registered executors. Custom declared return types still
need the production side-effect handler registration. Production signature
validation and runtime effect classification run before recording; unregistered
payloads, nil collection items, unclaimed nested collections and invalid event
wrappers fail without partial recording. `OpenReactorScenarioForID` and
`NewReactorScenarioForID` select explicit callback registrations without a Go
artifact type.

`Produced()` copies the outer slice but borrows payload objects. Do not mutate
those objects while asserting. Nested/cyclic collections are bounded at depth 64.
A green `Produced` assertion proves **only a classified returned payload**,
not serialization or transport acceptance: no event was appended and no command
executor ran. Direct side effects inside your handler still run;
inject test collaborators for those yourself.

## Make fidelity requirements explicit

Inspect `scenario.Fidelity().Substitutions()` and use `RequireFidelity` before
assertions that depend on a particular layer. `Fidelity.Require` is the
error-returning equivalent. Unknown layer names and any substituted layer fail
with `ErrFidelityUnavailable`; requiring all layers cannot silently succeed.
Layer availability identifies the execution boundary, not proof of a specific
behavior. You must still assert a real observable outcome.

A zero `Fidelity` also refuses every requirement. Kernel generation/schema
validation is not enabled by these fixtures and remains an unclaimed layer.

All fixtures honor `Config.Naming`; zero preserves production Go property names.
Select `chronicletest.CSharpScenarioNaming` explicitly for Fundamentals camelCase
(the C# scenario default, retaining leading acronyms). Explicit JSON tags win.
Registry compilation still rebinds schema, constraint and projection paths by
field identity. Naming does not change engine fidelity.

### Why the engines differ

The chosen boundary keeps production discovery and client execution shared while
leaving projection evaluation and invariant enforcement in their owning kernel.
An all-Go projection interpreter was rejected: it would be a second implementation
whose passing tests could disagree with Chronicle. The in-memory transport instead
dispatches directly to a deliberately small append/read substitute and rejects
unsupported operations. It copies protobuf messages across this boundary but
hosts no gRPC server and does not exercise HTTP/2.

C# can load the real kernel into its process; Go cannot claim that fidelity from
an in-memory map. Local scenarios therefore do not prove protection/key erasure,
durable storage, retries/quarantine or multi-node semantics. Even a real-kernel
projection replay is not evidence that a live observer caught up. Projection
fixtures also report delivery metadata and effect acceptance as unavailable: they
neither receive reactor deliveries nor execute returned effects. The
[kernel-backed siblings](../chronicletest/kernel_integration_test.go) separately
exercise projection replay, constraint rejection and live reactor lifecycle. Full
protected-data scenario coverage awaits the SDK's compliance surface; see the
[scenario parity map](parity.md#scenario-testing).
