# Go releases

This repository is in early development and has no tagged Go release yet. Repository setup uses `no-release`; the first implemented, minor-labeled release will be v0.1.0. Experimental releases remain v0.x until an approved stable launch.

## Release intent

Every PR has exactly one of `major`, `minor`, `patch`, or `no-release`. While v0.x, use minor for breaking experimental API changes and patch for compatible fixes. Describe breaking changes explicitly in the release notes. Dependabot PRs use `no-release`; a dependency change that needs publication requires a deliberate release-bound maintainer change.

The merged PR body becomes the GitHub Release notes verbatim. Use the [contribution guide](../CONTRIBUTING.md) and PR template, and keep verification and review notes in a PR comment.

## Publication

A push to main runs the Go and Markdown gates before preparing a release. The release action creates the immutable `vX.Y.Z` tag and GitHub Release only after its job succeeds. Dependent jobs read back the release and tag target, fetch the exact version through `proxy.golang.org`, verify the module archive, and request its pkg.go.dev page. Documentation rendering is asynchronous.

There is no separate registry upload or Go publishing credential. GitHub tags are Go module versions; the workflow uses its scoped `GITHUB_TOKEN` for GitHub writes. Keep one root module and the canonical lowercase module path. Release source must build without a developer workspace, sibling checkout, secrets, or local replacements.

Wait for Publish to finish before merging another release-bound PR: GitHub concurrency can replace a pending run even when running jobs are not cancelled.

## Major versions

`GO_RELEASE_MAJOR_CEILING` defaults to 0. Set it to 1 only for a maintainer-approved v1 launch and refresh the PR policy check. A major release must be reviewed and merged by a human. Values above 1 are rejected: Go v2+ requires a `/vN` module path, changed imports, and a separately reviewed release design. The current workflow does not publish prereleases or nested modules.

## Recovery

If proxy indexing fails after publication, rerun the failed jobs. A full rerun resolves the existing stable release for the same commit and indexes that tag without bumping again. Investigate any conflicting tag or release; never move or delete a published tag. Correct a bad release with a new version and, where appropriate, a Go `retract` directive.

The first public-proxy installation and pkg.go.dev visibility can be verified only after a release exists. Central Cratis documentation publication requires registering this repository in Cratis/Documentation and setting the repository variable `DOCUMENTATION_ENABLED` to `true`; it is independent of Go module indexing.
