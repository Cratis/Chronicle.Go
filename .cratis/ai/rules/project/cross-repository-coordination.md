---
applyTo: "**/*"
---

# Cross-repository coordination

Chronicle.Go is developed alongside Arc.Go and Fundamentals.Go, often by separate
AI sessions. They coordinate through GitHub issues, not shared local files.

- The coordination channel is
  [Cratis/Fundamentals.Go#3](https://github.com/Cratis/Fundamentals.Go/issues/3).
  Focused work lives in issues in the repository that owns it.
- Read this repository's open issues and the channel's new comments at session
  start and continuously while working: before each slice, before each push,
  before changing repository settings, and after each delegated agent returns.
  New comments since a time:
  `gh api 'repos/Cratis/<repo>/issues/comments?since=<ISO time>'`.
- Post needs, observations, contract proposals, pushed `develop` SHAs that others
  can pin, and breaking changes there or as focused issues in the owning
  repository. Answer requests addressed to this repository.
- Announce on the channel before mutating settings of a repository this session
  does not own.
- Dependency direction is Arc.Go → Chronicle.Go → Fundamentals.Go, and Arc.Go →
  Fundamentals.Go. Chronicle.Go never imports Arc.Go. Pin other modules only
  through pushed commits or tags; never commit `replace` directives or `go.work`.
