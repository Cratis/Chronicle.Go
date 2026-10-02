---
applyTo: "**/*"
---

# Third-party Go skills

Load repository-owned `go-*` skills first, then only the third-party skill relevant
for the task. These are supplementary references, not additional always-on rules.
[Go rules](../go.md) and [Cratis parity](../go-cratis-parity.md) override conflicts:
use the minimum from `go.mod`, no `pkg/`, and functional options only when warranted.
Read each skill's local qualifications before its preserved upstream examples.

| Skill | When to load |
| --- | --- |
| [jetbrains-go-modern-guidelines](../../skills/jetbrains-go-modern-guidelines/SKILL.md) | Modernizing touched syntax or stdlib APIs; offline version-gated catalog, no CLI installation |
| [spf13-go](../../skills/spf13-go/SKILL.md) | Broad library/package/API, testing, iterator or stdlib design review |
| [github-copilot-go](../../skills/github-copilot-go/SKILL.md) | Optional idiom, HTTP request ownership and I/O review checklist |
| [samber-golang-context](../../skills/samber-golang-context/SKILL.md) | Cancellation, deadline propagation and request metadata changes |
| [samber-golang-error-handling](../../skills/samber-golang-error-handling/SKILL.md) | Error identity, wrapping, inspection and logging-boundary review |

Each folder includes its upstream LICENSE and `UPSTREAM.md` with the full commit
pin, fetch date and modifications. Content is identical in both Go libraries.
These files are user-owned and unlisted in `.cratis/ai.manifest.json`. Updates
leave unlisted files alone unless a selected corpus asset collides with their path;
such collisions stop the update. Project instructions are a separate migration
case: the CLI splits `rules/project.md` at level-two headings when `rules/project/`
is absent. Keep that file heading-free and maintain concerns in this directory.
Never add local guidance to the managed manifest or use force to bypass a conflict.
See [repository project context](../../../../Documentation/project-context.md).

## Selection boundaries

Samber's design-patterns guide was not copied: application/DB architecture and
mandatory functional options add little over our library API skill. Its naming
skill imposes nonstandard boolean/in-place naming, its lint guide installs an
unpinned tool and suggests concurrent auto-fixes, and its concurrency references
contain uncancelable receives and unsafe lazy-resource lifecycle examples.
Use our existing skills for these topics. Other application, DB, infrastructure,
DI-container and vendor-library-specific skills are outside this curated set;
no extra HTTP skill is needed alongside `go-http-handlers` in Arc.Go and the shared
HTTP references. Do not auto-install missing upstream cross-referenced skills.
