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
| `EventScenario`, `Substitute` (zero-value engine) | Production client over a private bufconn append/read store | Constraints, concurrency enforcement, hashing, schema migration/validation, encryption, authorization, persistence or observers |
| `ReadModelScenario[M]`, reducer | Production fold plan in-process, one scope per source | Kernel delivery, storage, retries, quarantine or compliance |
| `ReadModelScenario[M]`, projection | Production compiler/encoder and real kernel bounded replay RPC | Materialized-sink catch-up, persistence or observer completion |
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

`New*` helpers call `t.Helper()` and register cleanup. `Open*` constructors instead
return `(scenario, error)` and require `Close`; use these outside `testing.T`.
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
joins and custom keys. `Instance` counts all returned roots before selecting one.
Keyed access requires the kernel response to include a nonempty string ID: declare
a root `ID` field or `chronicle:"key"` with a kernel-compatible `id`/`Id` JSON name.
If the schema drops the key, keyed access fails instead of guessing from seed order.

Pass `ReadModelOptions[M]{Projection: &declaration}` to replace a discovered
projection **or reducer** for this fixture. This uses `Registry.WithProjection`
to create a detached registry and then the ordinary production compiler. The
original registry is unchanged. Models with explicitly conflicting producer/sink
metadata still fail normal production validation. `Initial: &model` supports
reducer initial state; projection initial state is explicitly unsupported.

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
They are not a simulation of kernel replay or retry identities.

The recorder flattens collections, `eventsequences.Entry` and
`EventsWithConcurrencyScopes` to their payloads. It records returned commands
without running their registered executors. Custom declared return types still
need the production side-effect handler registration; recording does not bypass
signature validation. `OpenReactorScenarioForID` also selects explicit callback
registrations without a Go artifact type.

`Produced()` copies the outer slice but borrows payload objects. Do not mutate
those objects while asserting. Nested/cyclic collections are bounded at depth 64.
The recorder can accept shapes a production transport rejects: a green `Produced`
assertion proves **only the returned payload**, never that the event was appended
or a command was accepted. Direct side effects inside your handler still run;
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
whose passing tests could disagree with Chronicle. The bufconn transport instead
has a deliberately small contract and rejects unsupported operations.

C# can load the real kernel into its process; Go cannot claim that fidelity from
an in-memory map. Local scenarios therefore do not prove protection/key erasure,
durable storage, retries/quarantine or multi-node semantics. Even a real-kernel
projection replay is not evidence that a live observer caught up. The
[kernel-backed siblings](../chronicletest/kernel_integration_test.go) separately
exercise projection replay, constraint rejection and live reactor lifecycle. Full
protected-data scenario coverage awaits the SDK's compliance surface; see the
[scenario parity map](parity.md#scenario-testing).
