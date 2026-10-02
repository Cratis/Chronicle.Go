---
applyTo: "**/*"
---

Read [repository project context](../../../Documentation/project-context.md)
before work in this repository. It is the canonical repository-owned context
referenced by `general.md` under Project-Specific Instructions.

For Go source, `go.mod`, and `go.sum`, always apply [Go rules](go.md) and
[Cratis parity](go-cratis-parity.md). Select the matching Go skills from
`.cratis/ai/skills/`. This is a Go framework library, not an application or a C#
project. These local additions are intentionally absent from the managed manifest.

Read every project concern below before working in this repository. Together they
are project-owned instructions and override conflicting shared guidance.

- [Third-party Go skills](project/third-party-go-skills.md)
- [Local PR body checker patch](project/local-pr-body-checker-patch.md)
