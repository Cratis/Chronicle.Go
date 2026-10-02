---
name: go-library-api-design
description: Design or review exported Go APIs, constructors, interfaces, generics, iteration, ownership, and compatibility. Use when adding or changing public Go library surface.
---

# Go library API design

Read `Documentation/project-context.md`, `.cratis/ai/rules/go.md`, and
`.cratis/ai/rules/go-cratis-parity.md` before designing the public surface.
For transport details, load `go-grpc-client` or `go-http-handlers` as applicable.

## Workflow

1. Read `go.mod`, the existing callers, and the corresponding C# contract/tests.
   Sketch an external Go caller that accomplishes the same task with sane defaults.
2. Inventory what becomes public: types, methods, errors, options, interfaces,
   retained dependencies, wire formats, zero values, and concurrent-use guarantees.
3. Prefer concrete implementations and small consumer-owned interfaces. Preserve
   a deliberate public interface when applications must replace the collaborator.
4. Compare plain parameters, a config struct, and functional options. Options fit
   growing optional configuration; they also enlarge the namespace and need rules
   for invalid values, duplicates, and ordering. Do not require them everywhere.
5. Specify owned/borrowed resources, slice/map copying, callback lifetimes, nil
   behavior, repeated close, and whether user callbacks may run concurrently.
6. Use generics only for meaningful type safety/reuse. A slice fits finite results;
   `iter.Seq`/`Seq2` fits lazy traversal on Go 1.23+; an explicit stream handle fits
   acknowledgments and terminal errors. Never hide I/O failure as empty iteration.
7. Review compatibility: additions to interfaces, signatures, struct comparability,
   embedding, wrapped errors, defaults, and protocol shape can all break callers.
8. Add external-package tests and executable examples. Update documentation and
   `Documentation/parity.md`; identify the evidence for each parity claim.

## Small example: explicit option validation

Illustrative API, not an existing Cratis symbol. Last option wins; construction
validates the final configuration. Required dependencies should be parameters.

```go
package client

import "fmt"

// Client owns immutable client configuration.
type Client struct {
	buffer int
}

// Option configures a Client during construction.
type Option func(*Client)

// WithBuffer sets a positive queue capacity. The last value wins.
func WithBuffer(n int) Option {
	return func(c *Client) { c.buffer = n }
}

// New creates a Client with a default queue capacity of 64.
func New(options ...Option) (*Client, error) {
	c := &Client{buffer: 64}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("nil client option")
		}
		option(c)
	}
	if c.buffer <= 0 {
		return nil, fmt.Errorf("buffer must be positive: %d", c.buffer)
	}
	return c, nil
}
```

Do not embed a third-party client publicly merely to save forwarding methods;
that promotes its methods into your compatibility commitment.

## Verify and stop conditions

Run focused external-consumer tests, then the repository's required gates.
Use a pinned `gorelease`/`apidiff` if available, supplemented by behavior/wire tests.
Stop and identify unresolved ownership, wire, or lifecycle contracts before
publishing an API; missing required checks mean not ready, not compatible.

## References

- [Go review comments](https://go.dev/wiki/CodeReviewComments)
- [Module layout](https://go.dev/doc/modules/layout)
- [Compatibility](https://go.dev/blog/module-compatibility)
- [Generics](https://go.dev/blog/when-generics) and [iter](https://pkg.go.dev/iter)
- [Uber options guidance](https://github.com/uber-go/guide/blob/master/style.md):
  Uber Technologies, Inc., Apache-2.0; conceptual paraphrase, original example.
  See the attribution and license links in `.cratis/ai/rules/go.md`.
