---
name: go-grpc-client
description: Implement Chronicle.Go grpc-go clients, buf-generated contracts, connection ownership, metadata, deadlines, retries, and streaming subscriptions. Use for RPC wrappers, interceptors, generation, reconnects, and lifecycle tests.
---

# Chronicle Go gRPC client

Read `Documentation/project-context.md`, `.cratis/ai/rules/go.md`, and
`.cratis/ai/rules/go-cratis-parity.md`. Match the C# client in
`../Chronicle/Source/Clients/DotNET`, the server contracts, and their tests.
Use `go-concurrency` for goroutine ownership and `go-errors-and-context` for errors.

## Workflow

1. Read authoritative `.proto` definitions and C# client behavior first. Preserve
   field numbers, presence, service names, metadata, tenant selection, expected
   revision, and event/observer semantics. Never invent retry-safe operations.
2. Keep generated contracts in `contracts/`; use checked-in buf configuration
   and pinned Go/protobuf/gRPC plugins. `go generate ./...` must invoke the
   repository's buf recipe. Do not hand-edit generated code or fork contracts.
   Review generation diffs for unintended API/wire changes.
3. Prefer `grpc.NewClient` on grpc-go v1.63+. Share a `ClientConn` and generated
   clients; do not connect per operation. Construction is lazy, not a health
   check. Report RPC failure even after successful construction.
4. Distinguish owned from injected/borrowed connections. Close only what the SDK
   owns; define subscriptions' cancellation/join before connection shutdown.
5. Default remote connections to TLS with verified server identity. Explicit
   insecure transport belongs to development/test configuration. Inject credentials;
   do not put tokens in endpoints or log them.
6. Propagate per-call contexts. Unary operations need a documented deadline policy;
   subscription lifetimes must not inherit a short unary timeout accidentally.
7. Attach credentials, tenant/store identity, correlation, and causation using
   the actual contract's metadata keys. Merge outgoing metadata without mutating
   shared maps or dropping caller entries; binary keys require `-bin` handling.
8. Preserve/map gRPC status codes and details deliberately. Inspect statuses,
   not error strings; document whether callers can still inspect transport errors.
9. Separate transparent retries, configured gRPC retries, and application replay.
   Avoid stacked retry multiplication. Retry writes only with established
   idempotency/deduplication, especially after an ambiguous append timeout.
10. Configure keepalive only against server policy; aggressive pings can trigger
    rejection. Keepalive is not an RPC deadline or an application health signal.
11. Allow one concurrent sender and one concurrent receiver per stream, never
    multiple senders or receivers. `CloseSend` is not a complete stream shutdown;
    consume completion or cancel the stream context and join owned work.
12. Specify flow control, backpressure, terminal error/EOF handling, resume cursor
    inclusivity, duplicate delivery, acknowledgments, and checkpoint timing.
    Advance checkpoints only at the processing boundary the server contract defines.

## Small example: owned TLS connection

Illustrative construction helper, not the public Chronicle.Go API. Imports use
`grpc-go` v1.63+; the caller owns `Close`. Credentials for authentication are a
separate option from transport encryption.

```go
package transport

import (
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// NewConnection creates an owned, lazily connecting TLS channel.
// It does not prove server availability or successful authentication.
func NewConnection(target string) (*grpc.ClientConn, error) {
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	return grpc.NewClient(target, grpc.WithTransportCredentials(creds))
}
```

Do not add `InsecureSkipVerify`, dial inside each RPC, or reconnect manually
instead of using `ClientConn`'s connection management.

## Verify and stop conditions

Run `go generate ./...` when generation inputs change and inspect deterministic
output. Use `bufconn` tests for actual serialization, status details, interceptors,
metadata, cancellation, and borrowed-connection ownership. Real server/TLS tests
must cover guarantees an in-process server cannot establish.

Test append ambiguity, conflicts, canceled receive/send, reconnect duplicates,
early consumer exit, and cleanup. Run the repository's required gates. Record
unsupported semantics in `Documentation/parity.md`; stop rather than guessing
idempotency, checkpoint, or delivery guarantees.

## References

- [grpc-go anti-patterns](https://github.com/grpc/grpc-go/blob/master/Documentation/anti-patterns.md)
- [grpc-go concurrency](https://github.com/grpc/grpc-go/blob/master/Documentation/concurrency.md)
- [bufconn](https://pkg.go.dev/google.golang.org/grpc/test/bufconn)
- [Retries](https://grpc.io/docs/guides/retry/)
- [Keepalive](https://grpc.io/docs/guides/keepalive/)
- [Buf generation](https://buf.build/docs/generate/)

Original workflow/example; grpc-go source is Apache-2.0. gRPC website prose is
CC-BY-4.0 and its code samples Apache-2.0; no code samples are copied here.
