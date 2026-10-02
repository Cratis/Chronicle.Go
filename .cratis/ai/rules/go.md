---
applyTo: "**/*.go,go.mod,go.sum,**/go.mod,**/go.sum"
paths:
  - "**/*.go"
  - "go.mod"
  - "go.sum"
  - "**/go.mod"
  - "**/go.sum"
---

# Go engineering rules

These rules apply whenever Go source or module files are involved, not only
when requested. Read [project context](../../../Documentation/project-context.md)
and [Cratis parity](go-cratis-parity.md). Load the matching Go skill for depth.
Go framework libraries use Go tooling, not C# layout or test conventions.

## Version and scope

- Read `go.mod`, build constraints, and CI before choosing language features.
- The `go` directive is the minimum; `toolchain` is a development preference.
- Never raise the minimum Go version as an incidental modernization.
- Verify unfamiliar APIs against official documentation for that minimum.
- Keep experimental features opt-in; do not assume the newest compiler is CI.
- Test the supported minimum without silent toolchain upgrades.
- Do not rewrite unrelated code to modernize syntax.

## Formatting and naming

- Run `gofmt` on handwritten Go; use tabs as the formatter requires.
- Follow configured import grouping; avoid aliases unless they clarify a clash.
- Use MixedCaps for exported names and mixedCaps for unexported names.
- Preserve initialisms: `ID`, `HTTP`, `URL`, `JSON`, `RPC`.
- Use short names for short scopes and descriptive names for wider scopes.
- Avoid package stuttering; prefer `eventlog.Log` over `eventlog.EventLog`.
- Name packages for a responsibility, not `util`, `common`, or `misc`.
- Keep failure paths early and happy paths shallow; avoid clever compression.
- Use American English in identifiers, comments, and documentation.

## Packages and dependencies

- Start with a small public package surface aligned with caller workflows.
- Put implementation-only packages in `internal/`.
- Do not mandate `pkg/`, one type per file, or C# namespace-shaped directories.
- Keep optional adapters separate from core behavior when practical.
- Prefer the standard library where it satisfies the actual contract.
- Justify dependencies by capability, maintenance, license, and compatibility.
- Avoid initialization that performs I/O, starts goroutines, or mutates globals.
- Never hand-edit generated Go; update its input and pinned generation command.

## Public API contracts

- Design exported APIs from realistic external caller examples first.
- Export only behavior and types the library is prepared to support.
- Make zero values useful where practical; document required construction.
- Return construction errors for invalid configuration; do not defer surprises.
- Use ordinary parameters or config structs for simple configuration.
- Use functional options when extensibility outweighs namespace complexity.
- Specify option defaults, validation, duplicate handling, and ordering.
- Libraries must not call `os.Exit`, `log.Fatal`, or panic for normal failures.

## Documentation

- Document exported packages, types, functions, methods, fields, and constants.
- Begin doc comments with the declared name and explain the caller's contract.
- State nil/zero semantics, ownership, cancellation, and concurrency guarantees.
- Document relevant error identities, resource cleanup, and partial outcomes.
- Add executable `Example` functions for important public workflows.
- Use a `Deprecated:` paragraph with the replacement and migration guidance.

## Interfaces and types

- Define small interfaces at consuming boundaries, not for every concrete type.
- Keep public extension interfaces when Cratis substitution is the contract.
- Prefer concrete returns unless an abstraction is genuinely part of the API.
- Reuse standard interfaces such as `io.Reader` and `http.Handler`.
- Use named domain types to prevent meaningful identifier/value mixups.
- Use pointer receivers for mutation or values that must not be copied.
- Never copy a used mutex, wait group, or typed atomic value.
- Avoid public embedding that accidentally promotes dependency methods.

## Ownership and mutability

- Specify whether arguments and returned values are copied, borrowed, or owned.
- Copy slices/maps as needed; copying their headers does not copy contents.
- Do not expose mutable internal state or retain mutable caller config silently.
- Never return bytes backed by a buffer already returned to a pool.
- Document concurrent-use guarantees for clients, handles, and callbacks.
- Specify `Close` ownership, repeated close behavior, and use after close.

## Generics and iterators

- Use generics for meaningful type-safe reuse, not speculative abstractions.
- Keep interfaces for behavior that varies by implementation.
- Do not replace domain types with `any` just to simplify internal plumbing.
- Use supported `slices`, `maps`, and `cmp` helpers when they improve clarity.
- Check minimum-version support for generic aliases and newer syntax.
- Use `iter.Seq`/`iter.Seq2` only at a Go 1.23-or-newer minimum.
- Stop immediately when `yield` returns false; never call it again afterward.
- Release resources on exhaustion, early stop, cancellation, and failure.
- Always call the stop function from `iter.Pull` when iteration is finished.
- State ordering, single-use restrictions, and I/O error delivery explicitly.
- Keep an explicit stream handle when acknowledgment or `Close` is essential.

## Errors

- Return `error` last for ordinary failures; inspect every returned error.
- Explain intentional error suppression and preserve significant cleanup errors.
- Use sentinels for stable categories; typed errors for structured details.
- Return a nil `error`, not an interface containing a typed nil pointer.
- Add operation context without repeatedly restating the full call chain.
- Use `%w` only when exposing the wrapped error is intended public API.
- Preserve documented identities; use `errors.Is` and `errors.As`, not strings.
- Translate dependency errors when their identity must remain internal.
- Do not log and return the same error by default; handle it at one boundary.
- Do not hide partial success or ambiguous write outcomes behind retry helpers.

## Context and logging

- Put `context.Context` first on operations that block or support cancellation.
- Propagate it into HTTP, RPC, storage, callback, and retry operations.
- Never replace a caller's context with `Background` or `TODO` to escape it.
- Do not store request contexts in long-lived clients or options.
- Call every acquired cancel function on all applicable paths.
- Use context values for request metadata, never configuration or dependencies.
- Use private key types with typed accessors for application context values.
- Cancellation signals termination; it does not wait for goroutines to exit.
- Prefer injectable `log/slog` diagnostics at the supported Go baseline.
- Never configure the application's global logger from library code.
- Use context-aware logging and stable structured attributes; redact secrets.

## Concurrency

- Prefer synchronous APIs; let callers decide where concurrency belongs.
- Give every goroutine an owner, stop condition, and observable join path.
- Bound workers, queues, buffers, and retries; define overload/backpressure.
- Make blocking sends, receives, and waits cancellation-aware where needed.
- Only close a channel after all possible senders have stopped.
- Use mutexes for shared state and channels for coordination as appropriate.
- Do not hold internal locks while invoking user callbacks or doing slow I/O.
- Call `WaitGroup.Add` before launching work; `WaitGroup.Go` needs Go 1.25.
- Use `errgroup` when its cancellation/error behavior fits, not automatically.
- Check admission semantics: a saturated `errgroup.SetLimit` can block `Go`.
- Race-free access does not by itself prove deadlock-free or leak-free behavior.

## Transport and security

- Follow the repo's gRPC or HTTP skill before changing transport behavior.
- Preserve tenant boundaries, identity metadata, wire formats, and error shapes.
- Use TLS by default for remote RPCs; insecure development transport is explicit.
- Bound untrusted payload sizes, fan-out, and retained subscription state.
- Never retry writes without evidence of safe idempotency/deduplication.
- Separate long-lived subscription lifetimes from short unary-call deadlines.
- Specify resume positions, checkpoint timing, and duplicate delivery behavior.
- Do not promise exactly-once delivery without an end-to-end server contract.
- Do not log tokens, credentials, or entire sensitive event/command payloads.
- Use `crypto/rand`, not `math/rand`, for security-sensitive randomness.

## Tests

- Use standard `testing`; test contracts, not incidental implementation details.
- Add regression tests for corrected behavior, including error and boundary cases.
- Use table-driven cases only when setup and assertion logic genuinely match.
- Name `t.Run` cases meaningfully and report useful got/want failures.
- Use `t.Helper`, `t.Cleanup`, and `t.TempDir` for test support.
- Keep tests isolated before enabling `t.Parallel`; no shared mutable globals.
- Test public APIs from an external `_test` package where practical.
- Compare domain semantics; use protobuf-aware comparisons for generated types.
- Synchronize with channels/joins, not guessed sleeps or retry-until-green loops.
- Use stable `testing/synctest` at Go 1.25+ for isolated fake-time tests.
- Real networking is not fake-time synchronization; test it separately.
- Gate `T.Context` and `B.Loop` at Go 1.24; check actual cleanup ordering.
- Use `httptest` or `bufconn` plus real transport tests where needed.
- Cover cancellation, deadlines, early iteration stop, and owned-resource cleanup.
- Fuzz decoders/parsers with deterministic seeds; retain failing regression inputs.
- Bound fuzz duration explicitly; run `go test -race ./...` where supported.

## Checks and dependencies

- Profile or benchmark a demonstrated problem before optimizing.
- Do not introduce pools, caches, or unsafe code without measured justification.
- Run `go build ./...`, `go test ./...`, and the repository's required gates.
- Run `go vet ./...` and configured `golangci-lint run` checks.
- Pin a supported golangci-lint binary; do not enable every linter blindly.
- Run `govulncheck ./...` separately; lint is not vulnerability analysis.
- Run `go mod tidy` after authorized dependency changes and inspect its diff.
- Keep `go.mod`/`go.sum` consistent; avoid unnecessary dependency upgrades.
- Verify release consumption without local workspace/`replace` assistance.
- Report missing tools and failed checks honestly; never present them as passes.

## Compatibility and releases

- Prefer additive changes; exported signature changes and interface growth break.
- Check public struct comparability, embedding, errors, defaults, and wire shape.
- Use API diff tools as aids; they cannot prove behavioral compatibility.
- While v0, breaking changes require a minor release and migration notes.
- At v1+, breaking changes require a planned major release.
- Never introduce `/v2` without a migration and parallel-support plan.
- Use the configured release action and semantic root-module tags `vX.Y.Z`.
- Never move published tags; issue a corrective release and retract if needed.
- Record deliberate Cratis differences in `Documentation/parity.md`.

## Sources and attribution

This is original Cratis policy and examples, informed by the sources below.
Readability, error-boundary, option, and goroutine recommendations are paraphrased
and adapted, not copied code; version gates and Cratis policy are local additions.

- [Effective Go](https://go.dev/doc/effective_go),
  [review comments](https://go.dev/wiki/CodeReviewComments), and
  [doc comments](https://go.dev/doc/comment): Go contributors.
- [Google Go style](https://google.github.io/styleguide/go/guide.html),
  [decisions](https://google.github.io/styleguide/go/decisions.html), and
  [best practices](https://google.github.io/styleguide/go/best-practices.html):
  Google; adapted under [CC-BY-3.0](https://creativecommons.org/licenses/by/3.0/).
- [Uber Go style](https://github.com/uber-go/guide/blob/master/style.md): Uber
  Technologies, Inc.; consulted and paraphrased, no source code copied;
  upstream [Apache-2.0 license](https://github.com/uber-go/guide/blob/master/LICENSE).
- [Module compatibility](https://go.dev/blog/module-compatibility),
  [context](https://pkg.go.dev/context), [iter](https://pkg.go.dev/iter), and
  [synctest](https://pkg.go.dev/testing/synctest): Go contributors.
- [Vulnerability checks](https://go.dev/doc/security/vuln/) and
  [golangci-lint](https://golangci-lint.run/docs/welcome/install/ci/).
