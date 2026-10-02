# Upstream provenance

- Source: [JetBrains/go-modern-guidelines](https://github.com/JetBrains/go-modern-guidelines).
- Commit: `155dc7ca10da5e1f6c841503086957b1b37f5815`.
- Source path: [`plugin/skills/use-modern-go/SKILL.md`](https://github.com/JetBrains/go-modern-guidelines/blob/155dc7ca10da5e1f6c841503086957b1b37f5815/plugin/skills/use-modern-go/SKILL.md).
- License: Apache-2.0; the upstream [LICENSE](LICENSE) is included unchanged.
- Fetched: 2026-10-02, using `git clone --depth 1`.

## Local modifications

- Portable frontmatter contains only `name` (matching the folder) and a scoped
  `description`; added repository-rule precedence and local review qualifications.
- Replaced the CLI-only skill body with the offline workflow above; removed all
  executable installation, cache, wrapper, list/explain and authority instructions.
- Copied upstream `FEATURES.md` to `references/FEATURES.md`; added a prominent
  modified-file notice and corrected the `slices.Clip` backing-array claim.
- Qualified minimum versions, JSON wire compatibility, loop assignment semantics,
  map ranging, clone semantics, ticker lifetime, AfterFunc and routing changes.
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
