---
name: go-errors-and-context
description: Design Go error contracts, context propagation, cancellation, deadline budgets, and retry boundaries. Use when adding failures, wrapping dependency errors, or changing operation lifetime.
---

# Go errors and context

Read `Documentation/project-context.md` and `.cratis/ai/rules/go.md` first.
Load the transport skill for gRPC status mapping or HTTP response envelopes.

## Workflow

1. List failures callers must distinguish. Choose a sentinel for a category, a
   typed error for structured detail, or an opaque translation for private
   implementation failures. Document which identities callers can inspect.
2. Add operation context with `%w` only when the cause is intended public API.
   A future dependency swap must preserve exposed error semantics. Avoid both
   logging and returning the same error; log at the boundary making the decision.
3. Return a real nil `error` on success, not a typed nil in an interface.
   Preserve important cleanup errors, using `errors.Join` if the Go minimum
   supports it and multiple causes belong in the public contract.
4. Accept `context.Context` first and propagate it to blocking dependencies.
   Do not store request contexts in clients or replace canceled contexts with
   `Background`. Use context values only for request-scoped metadata.
5. Assign cancel ownership. Child timeouts cannot extend the parent's deadline;
   call their cancel functions on every path. Cancellation is not a goroutine join.
6. Set a total operation budget before retrying; keep attempts and backoff within
   it. Cancellation during a wait must terminate it. Retrying append after an
   ambiguous response needs demonstrated server idempotency, not optimism.
7. Define whether transport statuses remain inspectable or are translated. Keep
   domain rejection distinct from transport failure and partial success.
8. Test wrapping, `errors.Is`/`As`, nil success, caller cancellation, deadline
   expiry, cleanup, and ambiguous writes. Update public docs and parity entries.

## Small example: bounded propagation

Illustrative helper. It does not retry and deliberately exposes the callback's
error identity; the callback must honor its context.

```go
package operation

import (
	"context"
	"fmt"
	"time"
)

// Run executes call within both the parent's and the supplied time budget.
// Errors returned by call remain inspectable through errors.Is and errors.As.
func Run(ctx context.Context, budget time.Duration,
	call func(context.Context) error,
) error {
	if budget <= 0 || call == nil {
		return fmt.Errorf("positive budget and non-nil call required")
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := call(ctx); err != nil {
		return fmt.Errorf("run operation: %w", err)
	}
	return nil
}
```

A timeout cannot forcibly interrupt a callback that ignores its context. Do not
start an unjoinable goroutine to pretend this helper can enforce termination.

## Verification boundary

Run targeted error/deadline tests and the repository gates. Stop for an unresolved
retry guarantee or public error contract; do not guess transport semantics or
claim a timed-out write definitely failed on the server.

## References and attribution

- [Errors as values](https://go.dev/blog/errors-are-values)
- [Wrapping as API](https://go.dev/blog/go1.13-errors)
- [Context ownership](https://pkg.go.dev/context)
- [Google best practices](https://google.github.io/styleguide/go/best-practices.html):
  Google, [CC-BY-3.0](https://creativecommons.org/licenses/by/3.0/); error-boundary
  advice paraphrased for Cratis. Workflow and example are original.
