---
name: go-release-modules
description: Review Go module dependencies, minimum versions, compatibility, semantic labels, release-action tags, and proxy discoverability. Use for release preparation, retractions, and major-version migration planning.
---

# Go release modules

Read `Documentation/project-context.md`, `.cratis/ai/rules/go.md`, the repository's
PR rules, and the actual release workflow. Preparing a release does not authorize
pushing, tagging, publishing, or editing a released tag.

## Workflow

1. Establish the module root/path, current tag, intended next version, minimum
   Go version, and release-action configuration. Root modules here use `vX.Y.Z`.
   Nested modules, if ever introduced, require their own subdirectory tag prefix.
2. Review signatures, interface methods, struct comparability, wrapped errors,
   defaults, wire formats, and behavior. Use available pinned `gorelease` or
   `apidiff` tooling, but retain consumer and protocol tests.
3. Apply exactly one release-intent label: `major`, `minor`, `patch`, or
   `no-release`. While v0, breaking changes bump the minor (`v0.3` to `v0.4`),
   require migration notes, and must not hide behind `patch`. Verify that the
   action interprets the chosen label accordingly before merging.
4. At v1+, incompatible public changes require an explicitly planned major.
   Never introduce `/v2` without a migration/support plan: module directive,
   all affected imports, documentation, examples, downstream consumers, and
   parallel-maintenance policy must agree. Moving to v1 adds no `/v1` suffix.
5. For authorized dependency changes, run `go mod tidy`, inspect `go.mod` and
   `go.sum`, check dependency minimums/licenses, and run `govulncheck ./...`.
   No local-only `replace` or workspace may be necessary for downstream users.
6. Run `GOWORK=off go build ./...`, tests including race detection, lint, and
   the exact repository release gates. Verify generated contracts and examples.
   Test the minimum Go version, not just a newer auto-selected toolchain.
7. Let the configured `Cratis/release-action` workflow create `vX.Y.Z` tags.
   Do not manually duplicate its tag/release step. Check the action's actual
   inputs and workflow rather than assuming a label automatically publishes.
8. After an authorized release, verify the exact version can be downloaded
   without local replacements. A root tag plus a public repository is the Go
   publication mechanism; there is no separate package upload to invent.
9. Confirm proxy/pkg.go.dev indexing separately from source availability.
   Indexing can lag; never move a tag to fix caching or force rediscovery.

## Small example: verify a published version

Replace the illustrative version with the actual released tag. The first command
checks module metadata/content retrieval, not an external-consumer compilation.
Run a consumer build separately when verifying imported API usability.

```sh
GOWORK=off GOPROXY=https://proxy.golang.org \
  go mod download -json github.com/cratis/chronicle.go@v0.1.0
```

For Arc use `github.com/cratis/arc.go@<released-version>`. Inspect the command's
exit status and module/version fields; a cached local workspace build is not
publication evidence.

## Bad release recovery

Never delete, overwrite, or retag a published version; proxies and checksum
infrastructure make tags effectively immutable. Release a correction. If the
bad version should no longer be selected, add a documented `retract` directive
in a newer published version, explaining the affected range and replacement:

```gomod
retract v0.1.1 // Incorrect event metadata encoding; use v0.1.2 or later.
```

This is illustrative syntax, not a claim about any actual release. Retraction
does not erase downloads or prevent explicit selection; communicate migration.

## Stop conditions and references

Stop for an unclear breaking-change classification, v0 label/action mismatch,
missing required check, unapproved publishing action, or an unresolved `/v2`
migration. Report incomplete indexing without claiming a release failed.

- [Version numbers](https://go.dev/doc/modules/version-numbers)
- [Major versions](https://go.dev/doc/modules/major-version)
- [Publishing](https://go.dev/doc/modules/publishing)
- [Modules reference](https://go.dev/ref/mod)
- [Toolchains](https://go.dev/doc/toolchain)
- [gorelease](https://pkg.go.dev/golang.org/x/exp/cmd/gorelease)

Original Cratis release workflow guidance informed by official Go documentation.
