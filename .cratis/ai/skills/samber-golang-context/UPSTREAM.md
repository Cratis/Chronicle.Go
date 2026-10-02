# Upstream provenance

- Source: [samber/cc-skills-golang](https://github.com/samber/cc-skills-golang).
- Commit: `8e899e20ff0cd4dc524af3993e4c62d8ee8c5717`.
- Source path: [`skills/golang-context`](https://github.com/samber/cc-skills-golang/tree/8e899e20ff0cd4dc524af3993e4c62d8ee8c5717/skills/golang-context).
- License: MIT; the upstream [LICENSE](LICENSE) is included unchanged.
- Fetched: 2026-10-02, using `git clone --depth 1`.

## Local modifications

- Portable frontmatter contains only `name` (matching the folder) and a scoped
  `description`; added repository-rule precedence and local review qualifications.
- Annotated detached work, AfterFunc joining, response/error ownership and trace
  trust. Preserved all three reference files with local safety notes.
- Corrected processItems to use bounded synchronous work rather than unbounded
  goroutines and early return without joining. Corrected Charge's HTTP return
  arity in both good/bad examples and documented response-body ownership.
- Corrected the skill's ExecContext examples to return only the error, explicitly
  discarding the sql.Result rather than returning two values from an error-only API.
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
