# Contributing to Chronicle for Go

Thank you for helping build the Go client for Cratis Chronicle. This repository is in early development: the foundation implements connection, event registration and single append. Discuss larger changes before implementation, and document only capabilities that exist and have been verified.

The [Cratis contribution guide](https://github.com/Cratis/.github/blob/main/contributing.md) and [code of conduct](https://github.com/Cratis/.github/blob/main/CODE_OF_CONDUCT.md) apply.

## Before you start

- Open or identify a GitHub issue for the work; keep changes focused on it.
- This is a library, not an application. Do not add application-style domains or UI structure to the package.
- Match observable Chronicle behavior idiomatically in Go, rather than mechanically translating another language implementation.
- Do not claim kernel compatibility or feature parity without corresponding tests.

## Layout and setup

The runtime has one root module, `github.com/cratis/chronicle.go`, with package `chronicle`. The explicit [module allow-list](.github/go-modules.json) currently contains only that root; real nested tools, integrations or unpublished recipes can be added under the [module policy](Documentation/module-policy.md). Product documentation lives in `Documentation/`. Add packages and examples only as implementation needs them; use lowercase package directories, co-located `_test.go` files, and compiling `Example` tests for public usage.

Install Go 1.26 or later, golangci-lint v2.14.0, actionlint v1.7.12, ShellCheck, and markdownlint-cli2. CI tests Go 1.26 and 1.27, including the latest patches; golangci-lint must be built with a Go version at least as new as the code it analyzes.

## Verify your change

Validate the native module policy from the repository root, then run the Go checks inside each listed module with each supported toolchain where applicable. Root tests do not traverse nested modules:

```sh
python3 -B -m unittest discover -s .github/scripts -p 'test_*.py' -v
python3 -B .github/scripts/go_modules.py matrix
python3 -B .github/scripts/go_modules.py dependencies
```

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
go mod tidy -diff
actionlint -color
npx markdownlint-cli2 '*.md' 'Documentation/**/*.md' 'examples/**/*.md' '.github/ISSUE_TEMPLATE/*.md' '.github/pull_request_template.md' '!AGENTS.md' '!CLAUDE.md'
```

Race detection requires a supported platform and a C compiler. Run govulncheck with Go 1.27:

```sh
go install golang.org/x/vuln/cmd/govulncheck@v1.8.0
govulncheck ./...
```

Format handwritten Go source with `gofmt`; from the root, use `python3 -B .github/scripts/go_modules.py gofmt --module .` (or the listed nested directory) to check only that module's Git-visible Go files. Never hand-edit generated protobuf bindings. After authorized dependency changes, run `go mod tidy` and inspect `git status --short -- go.mod go.sum` inside the affected module. Commit its own `go.sum` when needed. Unlisted modules and repository workspaces are forbidden. All replacements are forbidden except the documented exact local-root replacement in expressly unpublished recipes; unpublished tool previews have no replacement exception. No nested publisher exists yet.

Hosted CI also runs the ordinary build, vet, and tests on macOS and Windows. Workflow lint invokes ShellCheck when it is available. Normal tests include parser/schema cases, real TLS/OAuth, bufconn RPCs, lifecycle and append behavior. The build gate also checks pinned contract generation and runs kernel integration tests. CodeQL runs separately in GitHub Actions.

## Contracts and kernel integration

Contracts live in this module under public `contracts/` packages. `contracts-source.json` pins Chronicle 19.32.3 at an immutable commit, input hashes and generator versions. Install Buf 1.73.0; Python 3 and Go are the other prerequisites. Generation installs its pinned Go plugins into isolated staging, fetches canonical upstream inputs and promotes only generator-owned files:

```sh
go generate ./...
python3 scripts/generate-contracts.py --check
```

The check also detects stale or unexpected generated files; it does not require .NET, protoc, a sibling checkout or a running kernel. Builds consume checked-in output and do not generate on demand. Review source pins, managed package mappings and generated diffs together when upgrading contracts. Never generate duplicate BCL/protocol packages in downstream clients.

Run the real-kernel tests against the development image matching the contract release:

```sh
docker run --rm --name chronicle-go -p 35000:35000 cratis/chronicle:19.32.3-development
# In another terminal, after https://localhost:35000/health reports Healthy:
CHRONICLE_INTEGRATION_CONNECTION_STRING=chronicle://localhost:35000 \
CHRONICLE_INTEGRATION_CONTAINER=chronicle-go \
  go test -tags=integration -count=1 -timeout=4m ./internal/integration
```

The tests create isolated random stores, validate registration and schema preservation, exercise numeric/nullable/dictionary round trips and concurrency bounds, and read persisted events through public contracts. Compliance tests also need the disposable encryption certificate from `scripts/configure-compliance-integration.sh chronicle-go`, and they inspect what the kernel stored by running `mongosh` inside the container named by `CHRONICLE_INTEGRATION_CONTAINER`. Stop your test container afterwards. A missing endpoint or container fails rather than silently skipping integration tests.

### Pinned capture parser checks

The optional capture AST test uses the real Screenplay 4.16.0 parser to check
Go-rendered translation syntax, identifiers, condition kinds and literal round
trips. It requires the .NET 10 SDK in addition to the normal Go/kernel setup:

```sh
dotnet build internal/integration/testdata/captureparser/CaptureParser.csproj \
  -c Release -p:BaseIntermediateOutputPath="$PWD/.ai-work/captureparser/obj/" \
  -o "$PWD/.ai-work/captureparser/bin/"
CHRONICLE_CAPTURE_PARSER_DLL="$PWD/.ai-work/captureparser/bin/CaptureParser.dll" \
CHRONICLE_INTEGRATION_CONNECTION_STRING=chronicle://localhost:35000 \
  go test -tags=integration -count=1 -timeout=4m ./internal/integration
```

Without `CHRONICLE_CAPTURE_PARSER_DLL`, only this optional AST test skips; kernel
capture validation still runs and checks compilation before capability errors.
The Go unit regressions remain independent of .NET and a sibling checkout.

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
