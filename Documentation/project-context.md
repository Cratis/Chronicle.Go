# Chronicle.Go project context

This is repository-owned project guidance, read through the shared corpus's
**Project-Specific Instructions** section. Chronicle.Go is a framework library,
not an event-sourced application or a port of the Chronicle kernel.

## Required guidance

For all Go/module work, read [Go rules](../.cratis/ai/rules/go.md) and
[Cratis parity rules](../.cratis/ai/rules/go-cratis-parity.md). Load the relevant
skills under `.cratis/ai/skills/`: `go-library-api-design`, `go-testing`,
`go-errors-and-context`, `go-concurrency`, `go-release-modules`, and
`go-grpc-client`. Apply Go conventions over C# syntax/layout/test conventions;
organization security and release gates still apply.

Optional [third-party Go skills](../.cratis/ai/rules/project.md#third-party-go-skills)
provide pinned JetBrains, spf13, GitHub Copilot and Samber references. Load by task,
not all at once; read their local qualifications. Repository Go/parity rules take
precedence. Each vendored folder retains its license and `UPSTREAM.md`; these are
unmanaged local additions shared with Arc.Go, not installer-managed files.

## Product and parity target

- Module: `github.com/cratis/chronicle.go`; root package: `chronicle`.
- Purpose: a Go client SDK for Cratis Chronicle's event store over gRPC.
- Primary reference: `../Chronicle/Source/Clients/DotNET`, associated C# client
  tests, and authoritative Chronicle protobuf contracts. Do not port kernel
  storage/Orleans internals into the client.
- Consult `../Chronicle.Kotlin` for cross-language translation experience, not
  as permission to diverge from the C# contract. Never edit sibling ports to
  accommodate Go.
- Maximize API, behavior, and developer-experience parity while using Go idioms.
  Document every deliberate difference and missing surface in
  [the parity map](parity.md); no parity claim without executable evidence.
- Arc.Go's Chronicle integration depends on this module; Chronicle.Go must not
  depend on Arc.Go or HTTP application hosting.

## Layout and commands

Keep one root module, public packages grouped by capability, implementation-only
helpers under `internal/`, and co-located `_test.go` files. Add directories only
when implemented; do not copy the C# `Source/` namespace layout. Public examples
should compile. Product documentation belongs in `Documentation/`.

Generated protobuf/gRPC bindings belong in `contracts/`, driven by pinned buf
configuration and Go generation directives. This describes the implementation
layout to establish, not a claim that generated contracts already exist.

Run from the root using the version in `go.mod` and the matrix in CI:

```sh
go build ./...
go test -race ./...
golangci-lint run
go vet ./...
govulncheck ./...
go generate ./...
```

`go generate ./...` regenerates contracts only after the buf recipe is installed;
a no-op invocation is not generation verification. Review its diff and never
hand-edit generated files. After authorized dependency changes, run `go mod tidy`
and inspect `go.mod`/`go.sum`. Use `GOWORK=off` to verify independent consumption.
The exact required gates and tool pins are in [CONTRIBUTING](../CONTRIBUTING.md)
and `.github/workflows/`; these quick commands do not replace them.

## Release policy

Exactly one PR label selects intent: `major`, `minor`, `patch`, or `no-release`.
The configured `Cratis/release-action` turns approved release intent into immutable
root-module tags `vX.Y.Z`; the merged PR body supplies the release notes.
While v0.x, breaking changes require a minor bump and migration notes; compatible
fixes use patch. A stable v1 launch needs explicit maintainer approval.

No `/v2` module or import changes without a migration/support plan and revised
release workflow. Never rewrite published tags; publish corrections and use
`retract` when appropriate. Confirm public proxy indexing separately from tagging.
Read [release policy](releases.md) and the `go-release-modules` skill before release
work. Preparing guidance or code does not itself authorize publication.

## Local AI ownership and harnesses

These Go rules, skills, and adapters are repository-owned additions, not entries
in `.cratis/ai.manifest.json`. Do not add managed markers or edit that manifest.
The distribution contract preserves unlisted local files on update; check
`cratis ai status` and conflicts before updating, especially if the upstream
corpus later adopts the same paths. Never use `--force` to resolve that silently.

- Claude reads the rules through `.claude/rules`.
- Codex, OpenCode, and pi reach this document through root `AGENTS.md` and its
  Project-Specific Instructions; the native skill directories expose Go skills.
- Copilot's `go.instructions.md` adapter explicitly loads the same rules/context
  through `.github/instructions`.
- Cursor's local `.mdc` adapters load the same canonical rules/context through
  `.cursor/rules`.

The Kotlin repositories use project-owned rules and a project entry point. Here,
`rules/project.md` points to this canonical document while existing managed root
entry points remain unchanged. Keep one set of facts here, not divergent harness
copies. Confirm instruction discovery in a fresh harness session after updates.
