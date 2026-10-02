# Upstream provenance

- Source: [github/awesome-copilot](https://github.com/github/awesome-copilot).
- Commit: `143a3d976b3c1603cc8932984d5e1f28501cb5fc`.
- Source path: [`instructions/go.instructions.md`](https://github.com/github/awesome-copilot/blob/143a3d976b3c1603cc8932984d5e1f28501cb5fc/instructions/go.instructions.md).
- License: MIT; the upstream [LICENSE](LICENSE) is included unchanged.
- Fetched: 2026-10-02, using `git clone --depth 1`.

## Local modifications

- Portable frontmatter contains only `name` (matching the folder) and a scoped
  `description`; added repository-rule precedence and local review qualifications.
- Converted `instructions/go.instructions.md` from applyTo instructions into an
  on-demand skill; removed the duplicate package-declaration warnings and retained
  one corrected clause permitting external tests and leading build constraints.
- Replaced the `pkg/` recommendation; corrected SQL sanitization advice to
  parameterized queries and safe context-specific handling.
- Added qualifications for error API exposure, receiver aliasing, pooling,
  replay/idempotency, pipe lifecycle, wire contracts and dependency choices.
- Upstream Markdown formatting is preserved rather than mechanically restyled.
  The existing `.cratis/**` ignore in `.markdownlint-cli2.jsonc` already covers
  this folder; no broader lint exclusions were added. Markdown CI does not glob
  this corpus. Validate metadata, reference paths and provenance separately.

## Maintenance

This is a reviewed snapshot, not a live dependency or an instruction to install
upstream tooling. Update deliberately: review the diff and license at a new
commit, recheck local qualifications, refresh this pin and fetched date, and keep
shared third-party content identical in Chronicle.Go and Arc.Go. Repository-owned
files remain absent from `.cratis/ai.manifest.json`; do not add managed markers.
