# Upstream provenance

- Source: [spf13/go-skills](https://github.com/spf13/go-skills).
- Commit: `9ac6eca43161163bb21621a520a57df50d5ad464`.
- Source path: [`go/SKILL.md`](https://github.com/spf13/go-skills/blob/9ac6eca43161163bb21621a520a57df50d5ad464/go/SKILL.md).
- License: MIT; the upstream [LICENSE](LICENSE) is included unchanged.
- Fetched: 2026-10-02, using `git clone --depth 1`.

## Local modifications

- Portable frontmatter contains only `name` (matching the folder) and a scoped
  `description`; added repository-rule precedence and local review qualifications.
- Moved the long original body into `references/go.md` with a local review
  pointer; replaced the entry point with scoped loading and explicit corrections.
- Annotated package/internal restrictions, generic/iterator absolutes, loop capture,
  concurrency limits, error wrapping, cmp.Or version, optional dependencies,
  detached contexts, testing cleanup, HTTP shutdown, logging and cache claims.
- Omitted upstream plugin metadata; no executable assets or dependencies added.
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
