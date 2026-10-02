---
applyTo: "**/*.go,go.mod,go.sum,**/go.mod,**/go.sum,**/*.proto,Documentation/**"
paths:
  - "**/*.go"
  - "go.mod"
  - "go.sum"
  - "**/go.mod"
  - "**/go.sum"
  - "**/*.proto"
  - "Documentation/**"
---

# Go ports: Cratis parity first

Preserve the C# product's concepts, behavior, developer experience, and wire
contracts while expressing them idiomatically in Go. This is a port, not an
opportunity to invent a parallel product. Read
[project context](../../../Documentation/project-context.md) for the target.

## Establish the contract

1. Read the relevant C# public API, documentation, implementation, and tests.
   Record the source path and revision in the parity entry for reproducibility.
2. Consult Kotlin for cross-language translation decisions and TypeScript for
   client-visible contracts. Neither silently overrides the C# parity target.
3. Write a Go caller example with the same concepts and workflow. Keep names
   recognizable; change spelling and construction patterns for Go, not meaning.
4. Preserve edge cases, defaults, sentinels, ordering, errors, lifecycle, tenant
   isolation, authorization, and extensibility, not just method signatures.
5. Add executable Go contract tests and update `Documentation/parity.md`.
   Reading source is evidence for a design, not proof of implemented parity.

Never change C#, Kotlin, or TypeScript implementations to make a Go port easier.
If the original contract is ambiguous or defective, describe the discrepancy and
seek an upstream decision separately; do not silently redefine it in Go.

## Translate concepts, not syntax

| Original concept | Go translation |
| --- | --- |
| `IEventLog` | `EventLog` interface when substitution is public contract |
| `Append` / `AppendAsync` | `Append(ctx, ...)` with a final `error` result |
| `Task<T>` / Kotlin `suspend` | Synchronous-looking `(T, error)` and context |
| Attributes / annotations | Explicit registration, typed helpers, struct tags |
| `ConceptAs<T>` | Named Go value type with validation and serialization |
| `Guid` / UUID identity | Approved UUID type, never an arbitrary string alias |
| `IObservable<T>` / `Flow<T>` | Channel, callback, or iterator with lifecycle |
| Constructor DI / service scopes | Constructors/options and explicit ownership |
| Assembly/classpath discovery | Explicit registration; optional safe helpers |
| Inheritance / virtual overrides | Composition and narrow extension interfaces |
| Generic fluent APIs | Supported type parameters or typed package functions |
| Nullable value / optional input | Deliberate presence representation |

- Dropping an `I` prefix or `Async` suffix must not drop a capability.
- Respect Go package naming without obscuring EventLog, Command, Query, Reactor,
  Projection, Observer, or other Cratis vocabulary.
- A named UUID-derived type does not inherit its underlying type's methods.
  Implement text/JSON conversion explicitly and test the canonical wire form.
- Preserve UUID byte ordering; do not assume .NET `Guid` bytes equal RFC bytes.
- Preserve units, timestamp precision, enum values, and numeric ranges.
- Model missing, null, empty, and zero separately when the original distinguishes
  them; do not blindly add `omitempty` or encode nil slices as null.
- Struct tags describe supported binding; they do not implement discovery.
- Validate registrations deterministically and reject duplicates/conflicts.
- Generics must work at the declared Go minimum; use package functions rather
  than inventing unsupported generic-method syntax.
- Use `iter.Seq` only for a pull-compatible workflow with explicit error delivery.
  A synchronous iterator alone does not recreate push subscriptions.
- For observables, specify ordering, backpressure, terminal errors, cancellation,
  unsubscribe, reconnection, and acknowledgment before choosing the Go shape.

## Wire compatibility is not optional

- Preserve camelCase JSON property names with explicit `json` tags where needed.
- Preserve routes, HTTP verbs, status codes, response envelopes, validation
  results, pagination/sorting, and observable-query framing.
- Preserve protobuf field numbers, names, enum values, presence, and RPC paths.
  Generate from authoritative contracts; never fork a schema merely for Go.
- Match the configured C# serialization contract, not C# property spelling or
  Go's default encoding. Use protobuf JSON rules only for protobuf JSON surfaces.
- Cover UUIDs, dates, decimals, large integers, and concepts with golden fixtures
  and cross-language consumer tests; default `encoding/json` is not proof.
- Preserve event metadata, expected revision, tenant/event-store selection,
  correlation/causation, and server-side conflict semantics.
- Do not add blind append retries, stronger delivery promises, or silent
  checkpoint advancement beyond what the server contract supports.
- SSE and WebSocket are not interchangeable wire formats. Implement the original
  observable protocol; offer another transport only as an explicit extension.

## Record every intentional difference

`Documentation/parity.md` is the durable compatibility map. Each affected entry
must name the C# source/revision, Go surface, status, executable evidence, and any
intentional deviation with rationale and caller migration implications.

Use **Not implemented**, **Partial**, **Implemented**, or **Go-specific**.
Unsupported behavior fails explicitly; a no-op success is not an implementation.
Do not mark a surface Implemented without a test that fails when its contract is
broken. Go-specific syntax may still preserve semantics; say exactly what differs.
A compile-only test does not demonstrate streaming, wire, or lifecycle parity.

Document deviations in the same change, including ergonomic substitutions such
as explicit registration. Do not call the whole product parity-complete because
selected endpoints or fixtures pass. Track missing behavior separately from
intentional design differences.

## Authority and references

The owning C# source/tests and documented protocol are the product authority;
read the sibling repositories without modifying them. Go translation follows
[Go rules](go.md), [module compatibility](https://go.dev/blog/module-compatibility),
[context](https://pkg.go.dev/context), and [iter](https://pkg.go.dev/iter).
This is original Cratis porting guidance; illustrative names are not a claim
that an API has already been implemented in either Go repository.
