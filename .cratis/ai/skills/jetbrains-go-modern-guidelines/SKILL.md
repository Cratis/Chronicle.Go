---
name: jetbrains-go-modern-guidelines
description: Choose version-compatible modern Go syntax and standard-library APIs. Load when modernizing touched Go code after reading go.mod and build constraints.
---

> Third-party guidance. Repository rules `.cratis/ai/rules/go.md` and
> `go-cratis-parity.md` take precedence where they conflict (e.g. Go minimum
> version from go.mod, no `pkg/`, functional options only when warranted).

# Modern Go guidelines (offline adaptation)

Modified by Cratis: the upstream CLI workflow is replaced with an offline,
version-aware reference workflow. No executable, wrapper, plugin, auto-install,
or toolchain-detection fallback is included.

1. Read the nearest owning `go.mod` and applicable file build constraints. The
   `go` directive is the supported minimum; `toolchain`, `go.work`, and the local
   compiler must not silently raise it. If no minimum is established, determine
   the supported version before selecting features.
2. Consult [the pinned feature catalog](references/FEATURES.md), considering only
   entries available at that minimum. Read the relevant explanation and example.
   Verify unfamiliar APIs against the official Go documentation for that version.
3. Modernize only the code in scope. Preserve public APIs, error identities,
   nil/empty distinctions, JSON wire shape, and lifetime behavior. A newer spelling
   is not automatically a safe replacement. Never raise `go.mod` incidentally.
4. Run the repository's relevant tests and gates. The catalog is third-party
   advice, not an authority overriding Cratis parity or official Go semantics.

## Local review qualifications

- Go 1.27 entries are not applicable to a Go 1.26 minimum, even with a newer
  development toolchain. Experimental APIs require explicit opt-in, not inference.
- New JSON code must also preserve the existing Cratis wire contract; v2 defaults
  are not permission to change missing/null/empty behavior.
- Per-iteration loop variables require declarations with `:=` under Go 1.22+
  semantics; variables assigned with `=` remain shared.
- Direct map ranging is already allocation-free; `maps.Keys` is useful for
  iterator composition, not a mandatory replacement for a simple map loop.
- `slices.Clip` only restricts capacity: it does not copy or release the underlying
  array. `Clone` is shallow; check nil-versus-empty behavior before substitution.
- Garbage collection of unused tickers does not terminate live polling loops.
  Every background task still needs cancellation and an observable join.
- `context.AfterFunc`'s stop function does not join an already-started callback;
  stopping it early may suppress cleanup. Specify ownership and synchronize it.
- `ServeMux` method/path matching can change routes and HEAD behavior. Test wire
  parity rather than mechanically translating manual path checks.

The reference retains upstream examples for review, not as tested SDK code.
