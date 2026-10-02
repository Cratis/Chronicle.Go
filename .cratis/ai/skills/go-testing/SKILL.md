---
name: go-testing
description: Write and review Go unit, contract, transport, race, fuzz, and deterministic concurrency tests. Use for new behavior, bug fixes, flaky tests, and public API parity evidence.
---

# Go testing

Read `Documentation/project-context.md` and `.cratis/ai/rules/go.md` first.
Use standard `testing`, not translated C# BDD infrastructure or coverage quotas.

## Workflow

1. Derive success, failure, boundary, cancellation, and cleanup cases from the
   contract. For a port, identify C# source/tests and stable cross-language data.
2. Choose small fakes at consumer boundaries and real implementations where
   affordable. Keep external-package tests for public workflows.
3. Use named table subtests when assertions are shared; otherwise split tests.
   Use `t.Helper`, `t.Cleanup`, and `t.TempDir`; isolate mutable state before
   `t.Parallel`. Use `T.Context` only at Go 1.24+ and heed cleanup ordering.
4. Compare errors with `errors.Is`/`As`, domain values semantically, protobufs
   with `proto.Equal` or protobuf-aware diffs, and wire output with contract
   fixtures. Do not rely on unstable map iteration or incidental error strings.
5. Use `httptest` for HTTP and `bufconn` for actual gRPC serialization/status
   tests. Add real networking/TLS coverage for guarantees those cannot prove.
6. Synchronize concurrent tests explicitly. At Go 1.25+, use `testing/synctest`
   for fake-time in-memory work; real network I/O is not durably blocked fake time.
   Test early consumer exit, canceled blocked operations, joins, and ownership.
7. Fuzz deterministic decoders with seed corpora, preserving discovered failures.
   Bound runs, for example `go test ./path -fuzz=FuzzDecode -fuzztime=15s`.
   Run one fuzz target/package at a time; ordinary tests still run fuzz seeds.
8. Run focused tests while iterating, then the repository's gates including
   `go test -race ./...`. Benchmarks require a question; `B.Loop` needs Go 1.24+.

## Small example: semantic table tests

This standard-library example demonstrates naming and error identity, rather
than claiming a Cratis parser API exists.

```go
package parsing_test

import (
	"errors"
	"strconv"
	"testing"
)

func TestParseInt(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  int64
		err   error
	}{
		{name: "valid", input: "42", want: 42},
		{name: "invalid", input: "no", err: strconv.ErrSyntax},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := strconv.ParseInt(tc.input, 10, 64)
			if !errors.Is(err, tc.err) {
				t.Fatalf("error = %v, want %v", err, tc.err)
			}
			if err == nil && got != tc.want {
				t.Errorf("value = %d, want %d", got, tc.want)
			}
		})
	}
}
```

Do not add guessed sleeps to make a flaky test pass. Also, an empty table or a
fixture reader that finds no files can pass vacuously: assert required case data.

## Verification boundary

Name checks actually executed and any required check that was unavailable.
A race pass only covers executed paths. A compile pass is not wire parity.
Do not promote parity status without a test that detects the claimed regression.

## References

- [Test comments](https://go.dev/wiki/TestComments)
- [Subtests](https://go.dev/blog/subtests)
- [Fuzzing](https://go.dev/doc/security/fuzz/)
- [synctest](https://pkg.go.dev/testing/synctest)
- [httptest](https://pkg.go.dev/net/http/httptest)
- [bufconn](https://pkg.go.dev/google.golang.org/grpc/test/bufconn)

Original workflow and example informed by Go project documentation.
