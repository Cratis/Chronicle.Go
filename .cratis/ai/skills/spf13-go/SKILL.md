---
name: spf13-go
description: Review Go library APIs, package boundaries, tests, iterators and standard-library choices. Load for broad design or idiom reviews after the repository-owned Go skills.
---

> Third-party guidance. Repository rules `.cratis/ai/rules/go.md` and
> `go-cratis-parity.md` take precedence where they conflict (e.g. Go minimum
> version from go.mod, no `pkg/`, functional options only when warranted).

# Idiomatic Go: local entry point

Read the qualifications below, then load the relevant sections of the
[pinned upstream guide](references/go.md). The long original skill body is kept
as a reference so discovery does not load the entire guide automatically.

## Local review qualifications

These qualifications override the corresponding upstream prescriptions:

- Package depth, flat layout and a three-type threshold for generics are
  heuristics, not Go rules. Use `internal/` for implementation-only packages;
  its import restriction is based on the parent directory tree, not simply module
  identity. An application's non-main packages can be imported unless protected.
- Preserve Cratis extension interfaces and typed generic APIs when the contract
  needs them. Do not reject an API merely because it resembles another language.
- A stateful domain entry point does not mandate reloading on every method call.
  Absence of globals alone does not make an object safe for concurrent use.
- Per-iteration capture applies to Go 1.22+ loop variables declared with `:=`,
  not pre-existing variables assigned with `=`. Validate `maxConcurrent > 0`;
  `errgroup.SetLimit` admission can block and is not context-cancelable.
- Mutexes, bounded workers and semaphores are valid choices; channels and errgroup
  are not universally preferable. All goroutines need ownership and joining.
- Explicit stream handles/Next methods are valid for I/O, terminal errors,
  acknowledgments and Close ownership. Iterators must honor early stop and cleanup.
- `cmp.Or` needs Go 1.22, not 1.21; it eagerly evaluates all arguments and treats
  zero as absent. Multiple `%w` operands are supported since Go 1.20; `errors.Join`
  is a choice, not a universal replacement. Wrap only intended public identities.
- Filesystem abstraction/Afero, go-cmp and table-driven tests are optional.
  Standard-library interfaces and `t.TempDir` often suffice; do not add dependencies
  merely because upstream calls one an industry standard.
- `T.Context` is canceled before cleanup callbacks run. `testing/synctest` does
  not make real networking deterministic. Test resource cleanup explicitly.
- `WithoutCancel` also drops deadlines. Never detach caller-owned SDK operations;
  explicitly independent work needs its own budget, owner, stop and join path.
- HTTP timeout examples are illustrative, not streaming defaults. Shutdown does
  not cancel active request contexts automatically or close hijacked WebSockets;
  explicitly signal long-lived handlers, close/join owned connections, handle
  shutdown failure, and join the serving goroutine. A handler context deadline
  cannot forcibly stop a handler that ignores it.
- Use an injected logger and context-aware methods; the upstream `slog.With`
  example actually derives from the global logger, not from context.
- Use `crypto/rand` for security, not either math/rand package. Preserve established
  JSON tags and presence semantics rather than adding `omitzero` mechanically.
- Version-gate every modern feature. Existing older-version build tags/tool
  tracking may need preservation. Never change the baseline to satisfy this guide.
- Go build/run can reuse valid cached compilation, and test results may be cached.
  Inspect inputs, build constraints and test-cache behavior before diagnosing;
  do not treat the toolchain as infallible or clear caches without evidence.
