---
title: Module policy
description: Module allow-list grammar, independent Go gates, unpublished previews and publication boundaries.
---

<!-- Copyright (c) Cratis. All rights reserved. -->
<!-- Licensed under the MIT license. See LICENSE file in the project root for full license information. -->

Keep optional tooling and ecosystem dependencies out of the Chronicle runtime.
The checked-in [module manifest](../.github/go-modules.json) currently lists only
`github.com/cratis/chronicle.go`. It does not create or publish tools, integrations,
or recipes. Use this reference when adding a real, independently compiled module.

## Manifest grammar

All fields are required. Unknown fields, duplicate JSON keys and wrong types
fail validation; the manifest cannot supply commands, arbitrary test exclusions,
module aliases, or executable configuration.

| Root field | Type and policy |
| --- | --- |
| `module` | Exactly `github.com/cratis/chronicle.go` |
| `go` | Exactly `"1.26"`; each module declares Go 1.26 or a 1.26 patch |
| `contracts` | Boolean `true`; pinned contract generation is root-only |
| `kernelIntegration` | Boolean `true`; the existing store/client kernel tests are root-only |
| `rootDependencies` | Unique canonical external module paths allowed in root `require` directives, including indirect and test dependencies |
| `nested` | An array of implemented module records; currently empty |

The runtime dependency list preserves the existing SDK graph: Fundamentals,
UUID, gRPC, protobuf and their current indirect modules. Versions remain in
`go.mod`/`go.sum`, not in this manifest. Adding a runtime dependency needs explicit
review of this allow-list; build tags and test-only imports do not isolate it.
OTel, Docker, brokers, application frameworks, configuration libraries and
`golang.org/x/tools` belong in separate modules, not in the runtime. Root `tool`
directives are forbidden too.

Each future nested record has exactly these fields:

| Nested field | Type and policy |
| --- | --- |
| `dir` | `tools`, `integrations/<name>`, or `recipes`; a real directory containing `go.mod` |
| `kind` | Respectively `tool`, `integration`, or `recipe` |
| `publish` | Boolean; `false` is CI-tested unpublished source, not package publication |
| `tagPrefix` | Exactly `<dir>/v`; a reserved namespace, not permission to tag |
| `contracts` | Boolean `false`; no nested contract-generation profile exists |
| `kernelIntegration` | Boolean `false`; no nested service profile exists |

Integration names use `[a-z][a-z0-9_-]*`, excluding Windows device names.
Directory aliases, absolute/escaping paths, backslashes, case variants, nested
module hierarchies, duplicate records and `/v2` suffixes are unsupported. The
module directive must be exactly `<root-module>/<dir>`; current policy supports
v0/v1 module identities only. Integration names such as `v2` cannot smuggle in a
major-version suffix. Recipes can never set `publish: true`.

The validator inventories tracked and nonignored untracked files through Git.
Every visible `go.mod` must match the allow-list; tracked files remain checked
inside ignored directories. Ignored local work, build output and downloaded
modules do not become modules merely because they contain a `go.mod`. Do not
ignore maintained module source to evade policy. Policy/module/checksum files
must be regular files with exact casing, without symlink ancestors or hardlinks.
Case-fold collisions are rejected. A root `go.work` is forbidden even if ignored;
visible nested workspaces are forbidden too.

## Unpublished source before a root release

Chronicle currently has no released root tag. This does **not** block ordinary
root CI or source implementation of declaration tooling in
[#23](https://github.com/Cratis/Chronicle.Go/issues/23) and
[#39](https://github.com/Cratis/Chronicle.Go/issues/39).

When there is real tool source, add `tools/go.mod` and its record together with
`kind: "tool"`, `publish: false`, `tagPrefix: "tools/v"` and both service profiles
`false`. The tool must require the canonical root through a fetchable, pushed
v0/v1 pseudo-version (or a stable released tag). It cannot replace the root with
local source, a remote fork or a sibling checkout. Root `go.mod`/`go.sum` stay
unchanged. Every native module gate still runs on this preview.

The dependency gate downloads that exact root version through the public proxy
in a fresh cache. A local-only commit cannot pass. A pseudo-version proves
available source, **not a release**. This explicit unpublished tool/integration
preview is the small Chronicle extension to Fundamentals' recipe-only unpublished
example; it breaks the pre-release circularity without weakening publication.

To become publication-eligible, flip `publish` to `true` only after pinning an
exact stable, publicly fetchable root tag. Pseudo-versions and prereleases fail
that policy. This flag still does not publish anything: no nested publisher is
included here, and the existing Publish workflow releases only the root.

## Recipe replacement exception

Only `kind: "recipe"`, `dir: "recipes"`, `publish: false` may use a replacement.
Its sole permitted form replaces the **unversioned canonical root** with the
canonical relative repository root, `..` or `../`. It requires the root at the
placeholder `v0.0.0` so tests exercise checked-out source. Both single-line and
parenthesized Go syntax are parsed by Go itself.

No third-party, remote, version-qualified, absolute, escaping, alternate sibling,
absent, normalized-alias or Windows-spelled replacement is accepted. Without a
replacement, recipes also require a fetchable root version. Every replacement
is forbidden in the root, tools and integrations, even unpublished previews.
Recipe source is not a supported import dependency and must never be published.
A future recipe needs an executable consumer, named owner, pinned profile and
upgrade policy; do not create placeholder modules or an adapter catalog.

## Independent native gates

From the repository root:

```sh
python3 -B -m unittest discover -s .github/scripts -p 'test_*.py' -v
python3 -B .github/scripts/go_modules.py matrix
python3 -B .github/scripts/go_modules.py dependencies
python3 -B .github/scripts/go_modules.py gofmt --module .
python3 -B .github/scripts/go_modules.py tidy --module .
```

The matrix contains module records with JSON booleans, not shell commands. CI
validates policy before starting module commands. Each listed module gets its
own build, vet, unit test, race, tidy, formatting, lint and vulnerability job,
with `GOWORK=off` and `GOTOOLCHAIN=local`. `ignore` directives in `go.mod` are
forbidden in every listed module, including unpublished previews and recipes:
they must not hide packages from the native gates. Go 1.26/1.27 Linux and Go 1.27
macOS/Windows lanes and the root's required check names are unchanged.

Run the [contribution guide's gates](../CONTRIBUTING.md) inside each listed module;
root `go test ./...` does not visit nested modules. Each module owns its own
checksums. `go mod tidy -diff` verifies without writing manifests. Formatting
partitions Git-visible Go files by owning module and preserves formatter errors.
Policy bounds JSON to 64 KiB and each `go.mod` to 1 MiB. It uses read-only
`go mod edit -json` with networking and automatic toolchain
selection disabled; it does not hand-parse `go.mod` or load dependencies.

Contracts and kernel integration keep their existing root-only workflows and
services. A real nested integration needing services requires a reviewed,
implemented profile and workflow, not arbitrary commands or named-test skips in
JSON. Policy, module lanes, workflow lint, contracts and integration all feed the
existing success-only `Go gate`; a failed or skipped dependency cannot pass it.

## Publication prerequisites and provenance

Before adding a publisher for a real module, mirror Fundamentals' protected
prefix, independent version/release-intent and retry/pre-existing-tag checks.
Protect `refs/tags/tools/v*` (or the integration directory's prefix) against
updates/deletion before release. A Git tag `tools/vX.Y.Z` corresponds to the Go
consumer version `github.com/cratis/chronicle.go/tools@vX.Y.Z`, not
`@tools/vX.Y.Z`. Require a full independent fresh-cache proxy download and an
installed consumer, without workspaces or replacements. Never move published tags.

This implementation adapts the corrected policy, tests and per-module CI pattern
from [Fundamentals.Go v0.2.0](https://github.com/Cratis/Fundamentals.Go/tree/532d2181c610f67730133f61e768a943379da571)
(`.github/scripts/go_modules.py`, `.github/workflows/build.yml` and
`Documentation/releases.md`), also unchanged in inspected `origin/main` at
`c138a1d52621f019b21324fd368e005769963273` for the layout policy. It preserves
Chronicle's runtime dependency policy rather than Fundamentals' stdlib-only root.
Only repository-owned native logic is adapted; no managed AI corpus or nested
publish workflow is copied.

[Fundamentals #16](https://github.com/Cratis/Fundamentals.Go/issues/16) records the
corrected exact recipe replacement guard and still-pending live nested-release
acceptance. Offline policy tests and workflow inspection do not prove tools tag
protection, hosted publication, proxy consumption, or installed-tool behavior.
Those checks remain pending for an actual implemented, authorized module release.
