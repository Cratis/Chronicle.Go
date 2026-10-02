# Chronicle.Go parity

The target is maximum API, behavior, and developer-experience parity with the C#
client in `../Chronicle/Source/Clients/DotNET`, expressed idiomatically in Go.
This initial map describes the target, not demonstrated implementation parity.

## Status and evidence

- **Not implemented**: no working Go surface yet.
- **Partial**: only named behavior has executable evidence; list the gaps.
- **Implemented**: the named contract has tests that detect its regression.
- **Go-specific**: a deliberate Go shape or behavior; explain the deviation,
  rationale, compatibility impact, and executable evidence separately.

Do not promote a status from source reading, compilation, or an empty test run.
Each implemented entry must identify the C# source/revision, Go symbols, and
concrete tests. Preserve unresolved behavior explicitly.

## Initial scope

| Surface | Status | Target and evidence required |
| --- | --- | --- |
| Event-log operations | Not implemented | C# client semantics and RPC tests |
| Typed events and concepts | Not implemented | Identity and wire fixtures |
| Observers/subscriptions | Not implemented | Lifecycle, resume, and error tests |
| Generated gRPC contracts | Not implemented | Pinned buf generation and RPC tests |
| Overall C# client parity | Not implemented | Surface-by-surface evidence |

## Intended Go translations

Context-first operations with final errors, named domain types, explicit typed
registration, and constructors/options replace C# async methods, concepts,
attributes/discovery, and dependency injection. These are planned translations,
not implemented capabilities. Add individual entries with source revisions,
rationale, exact semantic differences, and migration guidance when implementing.

Do not assume append retries are safe, checkpoints are interchangeable, or
exactly-once delivery exists. Establish those guarantees from the server contract
and test them. Follow the [porting rules](../.cratis/ai/rules/go-cratis-parity.md).
