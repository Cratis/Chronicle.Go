# Contributing to Chronicle for Go

Thank you for helping build the Go client for Cratis Chronicle. This repository is in early development: it contains a module scaffold, not client APIs. Discuss larger changes before implementation, and document only capabilities that exist and have been verified.

The [Cratis contribution guide](https://github.com/Cratis/.github/blob/main/contributing.md) and [code of conduct](https://github.com/Cratis/.github/blob/main/CODE_OF_CONDUCT.md) apply.

## Before you start

- Open or identify a GitHub issue for the work; keep changes focused on it.
- This is a library, not an application. Do not add application-style domains or UI structure to the package.
- Match observable Chronicle behavior idiomatically in Go, rather than mechanically translating another language implementation.
- Do not claim kernel compatibility or feature parity without corresponding tests.

## Layout and setup

The repository has one root module, `github.com/cratis/chronicle.go`, with package `chronicle`. Product documentation lives in `Documentation/`. Add packages and examples only as implementation needs them; use lowercase package directories, co-located `_test.go` files, and compiling `Example` tests for public usage.

Install Go 1.26 or later, golangci-lint v2.14.0, actionlint v1.7.12, ShellCheck, and markdownlint-cli2. CI tests Go 1.26 and 1.27, including the latest patches; golangci-lint must be built with a Go version at least as new as the code it analyzes.

## Verify your change

Run from the repository root, with each supported Go toolchain where applicable:

```sh
export GOWORK=off
export GOTOOLCHAIN=local
go mod download
go mod verify
go build ./...
go vet ./...
go test -count=1 -timeout=2m ./...
go test -race -count=1 -timeout=3m ./...
golangci-lint run
go mod tidy
git diff --exit-code -- go.mod go.sum
actionlint -color
npx markdownlint-cli2 '*.md' 'Documentation/**/*.md' 'examples/**/*.md' '.github/ISSUE_TEMPLATE/*.md' '.github/pull_request_template.md' '!AGENTS.md' '!CLAUDE.md'
```

Race detection requires a supported platform and a C compiler. Run govulncheck with Go 1.27:

```sh
go install golang.org/x/vuln/cmd/govulncheck@v1.8.0
govulncheck ./...
```

Format Go source with `gofmt`; no files should appear in `gofmt -l doc.go` for the initial scaffold. Check all Go files as the codebase grows. After `go mod tidy`, also check `git status --short -- go.mod go.sum` for untracked manifests. Commit `go.sum` when dependencies require it. Do not commit nested modules, local `replace` directives, or personal `go.work` files: released modules must build without sibling checkouts.

Hosted CI also runs the ordinary build, vet, and tests on macOS and Windows. Workflow lint invokes ShellCheck when it is available. The scaffold has no behavioral or integration tests yet; a passing empty package is not evidence of product compatibility. Add tests with behavior, and explicitly bounded integration checks before claiming kernel compatibility. CodeQL runs separately in GitHub Actions.

## Conventions

- Use American English and idiomatic Go, including context cancellation and explicit error handling.
- Start source files with the Cratis copyright and MIT license header, as in `doc.go`.
- Let `gofmt` control Go formatting; use `.editorconfig` for other files.
- Document exported APIs and update `Documentation/` with compiling examples when those APIs exist.

## Pull requests and releases

- Use focused conventional commits and merge commits; do not squash, rebase, or force-push shared history.
- Apply exactly one release-intent label: `major`, `minor`, `patch`, or `no-release`. Setup-only changes use `no-release`; Dependabot uses `no-release` too.
- Keep the PR body user-facing: optional `## Summary`, then only applicable `## Added`, `Changed`, `Fixed`, `Removed`, `Security`, or `Deprecated` sections, with concise bullets. End a delivered issue's bullet with `(#123)`; use `(part of #123)` if it stays open. Delete placeholders and unused sections, use absolute links, and put test/review notes in a PR comment.
- The PR body is published verbatim as release notes. The first minor release becomes v0.1.0. During v0.x, use minor for breaking experimental API changes and describe the break explicitly; use patch for compatible fixes.
- A major release requires human approval. `GO_RELEASE_MAJOR_CEILING` defaults to 0, blocking an accidental v1 launch. A maintainer can set it to 1 for an approved v1 release; v2+ requires `/vN` module/import paths and a revised workflow.
- Wait for Publish to finish before merging the next release-bound PR. Tags are immutable; never delete or move a released version. See [release policy](Documentation/releases.md).

## AI-assisted contributions

Managed Cratis AI rules and harness adapters are not hand-edited. Shared improvements belong in [Cratis AI](https://github.com/Cratis/AI); project-specific guidance belongs under `.cratis/ai/rules/project/`.

Plans, scratch files, and work records belong only in the ignored `.ai-work/` directory and are never committed. Durable follow-ups belong in GitHub issues.

## Security

Do not report vulnerabilities in public issues. Follow [SECURITY.md](SECURITY.md).
