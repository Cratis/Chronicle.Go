---
applyTo: "**/*.go,go.mod,go.sum,**/go.mod,**/go.sum,Documentation/**"
paths:
  - "**/*.go"
  - "go.mod"
  - "go.sum"
  - "**/go.mod"
  - "**/go.sum"
  - "Documentation/**"
---

# Go instruction adapter

Read and follow `Documentation/project-context.md`, `.cratis/ai/rules/go.md`,
and `.cratis/ai/rules/go-cratis-parity.md` from the repository root.
Load the relevant `go-*` skill under `.cratis/ai/skills/` before specialized work.

This repository-owned adapter supplies Copilot's native `.instructions.md`
suffix through `.github/instructions`; policy stays in the canonical files.
