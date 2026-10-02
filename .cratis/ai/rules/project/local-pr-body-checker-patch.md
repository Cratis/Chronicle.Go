---
applyTo: "**/*"
---

# Local PR body checker patch

Pending [Cratis/AI#478](https://github.com/Cratis/AI/issues/478), the managed
[PR body checker](../../hooks/scripts/cratis-check-pr.mjs) has its shebang on
line one and its `cratis-ai-managed` marker on line two. The installer currently
prepends the JavaScript marker above the shebang, which causes a Node syntax error
and blocks the [Claude PR body guard](../../hooks/scripts/cratis-guard-pr-body.sh).
The guard also has execute permission because the Claude settings invoke it
directly; the installed copy lacked that permission. Preserve it during updates.
This is a narrowly scoped exception to the no-hand-edits rule for managed files.

The installer determines ownership from `.cratis/ai.manifest.json`, not the
marker's line number, and hashes the entire installed content. Moving the marker
therefore makes this checker a locally edited managed file. `cratis ai update
--dry-run -o json` reports `hooks/scripts/cratis-check-pr.mjs` as a conflict and
stops before applying updates. Do not change the manifest hash or use `--force`
to hide or overwrite this patch. Once the installer fix is released, review the
replacement and retire this exception through a deliberate update.
