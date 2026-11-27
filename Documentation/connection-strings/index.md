---
title: Connect to Chronicle
description: Configure Chronicle endpoints, TLS, OAuth credentials and client ownership in Go, and learn how a connection string maps onto client options.
---

A client shares one connection generation across stores and namespaces. Create it with `chronicle.NewClient(options...)` for lazy construction, or `chronicle.Dial(ctx, options...)` to finish startup before returning. With normal logical sessions, startup includes authentication, structural compatibility and the first kernel keep-alive. With `WithSkipKeepAlive`, the protected read-only startup probe replaces that heartbeat; compatibility is still checked unless explicitly skipped. See [short-lived clients and process ownership](lifecycle.md#short-lived-clients-and-process-ownership) for exclusions and readiness limits.

## Connection strings

The default endpoint is `chronicle://localhost:35000`. Supply another endpoint with `chronicle.WithConnectionString`. Credentials use OAuth client credentials, not HTTP Basic:

```text
chronicle://client-id:percent-encoded-secret@kernel.example:35000
chronicle://[::1]:35000
chronicle://localhost:35000?auth=none
```

Percent-encode reserved characters in credentials. Omitted credentials select the public development client `chronicle-dev-client` / `chronicle-dev-secret`; use explicit credentials or `WithTokenSource` in production. `WithNoAuthentication` and `auth=none` suppress SDK token exchange and authorization metadata. Combining authentication modes fails construction.

`ParseConnectionString` performs no I/O. `String()` and Go formatting redact credentials by displaying only scheme and addresses. Errors never echo the input URI. Option names, boolean values and `auth=none` are case-insensitive, like C#. Percent escapes are decoded without changing literal `+` characters into spaces. Unknown query options and duplicate query keys (including case variants) fail instead of becoming ignored configuration.

Use `chronicle://one:35000,two:35000` for multiple endpoints or `chronicle+srv://cluster.example` for `_chronicle._tcp.cluster.example` discovery. SRV records are resolved again on each generation, ordered by priority then descending weight like C#. A connection accepts at most 64 endpoints/records; SRV accepts one seed name and must return a nonempty set. `srvNameServer=127.0.0.1:53` selects a DNS server; `WithSRVResolver` supplies a caller-owned, cancelable resolver.

Only use `chronicle+srv://` with DNS infrastructure and SRV records you trust, ideally DNSSEC-protected. The selected SRV target becomes both the gRPC endpoint and, with built-in OAuth client credentials, the token authority. TLS certificate validation verifies that target's identity; it does not establish that DNS selected an intended server. Use explicit server addresses when you cannot trust discovery.

`loadBalancer=least-connections` is the default. It probes HTTPS `/connections/count` with a two-second budget per endpoint, randomly breaks ties, and makes a best-effort `/connections/reserve` request. Probes run concurrently over the bounded endpoint set after up to 250ms of jitter. Reservations expire server-side after 30 seconds; there is no release endpoint. `loadBalancer=round-robin` starts at a random offset; `loadBalancer=random` independently selects a candidate. Selection happens per generation, not per append. Single endpoints need no probe.

`apiKey=<key>` sends the key as `api-key` metadata on every RPC instead of exchanging a token, and it conflicts with credentials, `auth=none`, `WithNoAuthentication` and `WithTokenSource`. `disableTls=true` dials plaintext, also for token exchange and load balancer probes; it suits a kernel behind something that terminates TLS. Chronicle serves its token endpoint over TLS only, so a cleartext connection without explicit credentials authenticates with nothing, like C#. `skipKeepAlive=true` behaves like `WithSkipKeepAlive()` and conflicts with an explicit keepalive timeout. `skipCompatibilityCheck=true` behaves like `WithSkipCompatibilityCheck()`.

`caCertificatePath=<file>` names a PEM file with the certificate authorities the server certificate must chain to, replacing the system trust store. `certificatePath=<file>` names a PEM file holding the client certificate chain and an unencrypted private key for mutual TLS, with the leaf certificate first. Both are read when the client is created, also while validation is skipped or TLS is disabled, so a missing or unreadable file, or one without usable PEM, fails with `ErrInvalidConfiguration`; errors never echo the path or file contents. Authorities apply only while validation is on. They conflict with an explicit `WithTLS` policy and with a borrowed connection, and `disableTls=true` conflicts with `certificatePath` and with `WithTLS`.

`certificatePassword` is recognized but returns `ErrUnsupported`: the standard library reads neither PKCS#12 nor encrypted private keys. Use an unencrypted PEM file, or load the material yourself and pass it through `WithTLS`.

## Custom load balancing

Supply `WithLoadBalancer(balancer)` to choose an endpoint with your own
`LoadBalancer.Next(ctx, candidates) (ServerAddress, error)` implementation. Last
option wins. The client borrows the strategy and never closes it, even if it
implements `io.Closer`. A strategy shared by multiple clients must support their
concurrent calls.

This strategy excerpt uses `context` and
`chronicle "github.com/cratis/chronicle.go"` imports:

```go
type preferLastServer struct{}

func (preferLastServer) Next(ctx context.Context, candidates []chronicle.ServerAddress) (chronicle.ServerAddress, error) {
    if err := ctx.Err(); err != nil {
        return chronicle.ServerAddress{}, err
    }
    return candidates[len(candidates)-1], nil
}
```

Pass `chronicle.WithLoadBalancer(preferLastServer{})` to `NewClient` or `Dial`.

Each connection attempt calls `Next` once, including single-address URIs, first
connect and every reconnect. Calls are serialized per client, outside SDK locks.
Candidates are a fresh copy in URI order, or priority/descending-weight order
after SRV re-resolution. You may retain or modify the copy, but must return one
of the original candidates. That address becomes both gRPC endpoint and OAuth
authority. Honor the attempt context: its deadline covers `WithConnectTimeout`
and client close cancels it. Do not call Close/Shutdown from `Next`.

Without an explicit strategy, the URI strategy applies (default
least-connections). Unlike C#'s explicit-over-URI precedence, Go rejects combining
`WithLoadBalancer` with a `loadBalancer=` URI key or `WithGRPCConnection`, whose
owner controls routing. Nil and typed-nil strategies also fail construction with
`ErrInvalidConfiguration`, before I/O.

Errors, contained panics and non-candidate results become `*LoadBalancerError`;
no endpoint is dialed and no OAuth request is made. Its `Error()` and formatting
are fixed diagnostics, while `Unwrap()` preserves ordinary causes for inspection.
Panic values are discarded. First `Connect` returns a non-context selection
failure. A selection error matching a context error is treated like an attempt
timeout: `Connect` keeps waiting on its own context while the supervisor retries.
The supervisor retries subsequent attempts with normal jittered reconnect backoff.
Selection failures are always transient, even when wrapping `Unauthenticated`.

[The executable custom-balancer example](https://github.com/Cratis/Chronicle.Go/blob/main/example_connection_test.go)
shows a strategy choosing the last candidate without network I/O.

## TLS and token sources

Certificate validation is **enabled by default**, unlike the C# development default. `WithDevelopmentDefaults()` explicitly permits a self-signed local kernel certificate. `skipTlsValidation=true` is an explicit URI alternative. Neither overrides an explicit `WithTLS` policy; `skipTlsValidation=false` also wins over development defaults.

`WithTLS(*tls.Config)` clones the configuration and its root pool for both OAuth HTTPS and gRPC. Set `RootCAs` with `x509.NewCertPool`/`AppendCertsFromPEM`; load client certificates with `tls.LoadX509KeyPair`. TLS 1.2 is the minimum. Treat supplied certificates, private keys and TLS callbacks as immutable after configuration. Password-protected certificate files are unsupported; URI certificate options never silently succeed (see above for which error each returns). A URI `caCertificatePath` or `certificatePath` is the shorthand for the same material.

`WithTokenSource(source)` accepts a concurrent, context-aware `Token(ctx) (chronicle.Token, error)` implementation. You own that source; the client never closes it. Nonzero expiration must be in the future. Tokens format as redacted, but their `AccessToken` field is sensitive.

Built-in OAuth sends a form POST to the selected kernel's `/connect/token`, caches tokens, refreshes within one minute of expiry and serializes refreshes. A failed refresh can serve a still-valid token; failed exchanges are throttled for five seconds. The default lifetime without `expires_in` is 3600 seconds. Expired credentials, redirects and malformed responses fail closed. Token requests have a five-second budget. An `Unauthenticated` unary or stream failure invalidates the cached token, including its failed-refresh fallback. The next credential acquisition obtains a fresh token; no application operation is automatically retried. Every replacement generation acquires OAuth credentials from its newly selected endpoint. External sources can implement the concurrency-safe, nonblocking `TokenInvalidator.Invalidate()` contract for the same notification.

## Lifecycle and cancellation

- Client, store and sequence handles support concurrent calls. Caller event values must not be mutated during serialization.
- `WithConnectTimeout` defaults to five seconds and never extends a shorter caller deadline.
- Unary operations use the caller's deadline, not a hidden short timeout. Always supply a bounded context for external I/O.
- Concurrent `Connect` calls share startup. Its initiating context bounds that first attempt; other waiters can cancel independently. Later generations use only client-owned lifetimes. Store registrations are single-flight per generation; failed passes remain inspectable and can recover.
- `EventStore(ctx, name, WithNamespace("tenant"))` caches by both store and namespace. The default namespace is `Default`, matching C# and the kernel (case matters). Namespace selection is not authorization.
- `EvictEventStores()` clears all lookup/automatic-registration membership without closing issued handles or resources. Later lookups return new facades; retained handles can still perform explicit operations. See [eviction semantics](lifecycle.md#evict-cached-event-stores).
- `WithOnConnected` and `WithOnDisconnected` observe protocol readiness/loss asynchronously, not artifact registration. See [connection hook ordering and ownership](lifecycle.md#connection-hooks).
- `Close()` cancels and joins owned work. `CloseContext(ctx)` bounds that wait; `Shutdown(ctx)` first drains admitted RPCs. A context error means cleanup is incomplete, not successful. Later operations return `ErrClosed`. A custom source that ignores cancellation can delay actual cleanup.
- `WithGRPCConnection(conn)` borrows a channel; you own its security, retry policy and eventual close. The SDK still applies its own admission, metadata and compatibility checks. Multihost/SRV selection on a borrowed channel is rejected because its owner controls routing. You must explicitly select `WithConnectionString` as the OAuth authority, `WithTokenSource`, or `WithNoAuthentication`; the SDK never guesses a localhost authority for a borrowed channel. Use `WithNoAuthentication` if the supplied channel owns credentials. Do not enable application-level append retries on a borrowed channel.

With normal logical sessions, transient stream loss or missing heartbeats triggers automatic reconnection and registration replay. Existing handles remain usable after recovery. Authentication rejection and incompatible protocols stop supervision until an explicit `Connect`; readiness never silently bypasses those failures. `WithSkipKeepAlive` omits that session and its loss detection; transport reconnection alone does not guarantee registration replay. Read [short-lived clients and process ownership](lifecycle.md#short-lived-clients-and-process-ownership) before selecting it, and [connection lifecycle and readiness](lifecycle.md) for admission, retry and shutdown contracts.

On the first automatic replacement attempt after an established session drops,
Go's built-in balancer excludes the dropped address if the current candidate
list contains a different address. If that attempt fails transiently, the next
attempt uses the full current list, resolving DNS again for SRV. The dropped
address is eligible then, not guaranteed to be next. A terminal authentication
or compatibility rejection on the selected peer still stops supervision; the
client does not silently try another node. Custom balancers still receive the
full candidate list, and borrowed channels are unaffected.

Unlike Go's one-attempt preference, C# remembers the last dropped address across
attempts and can try multiple candidates in one connect budget, withholding the
dropped address from built-in and custom selection while other candidates remain.
See [the parity comparison](../parity.md#connection-api-and-default-comparison).

## Diagnose connection failures

Check kernel health with `curl` against `/health`, then inspect the returned error. Use `errors.As` for `CompatibilityError`, `LoadBalancerError` and `EnvelopeError`; use `errors.Is` for `ErrClosed`, `ErrInvalidConfiguration`, `ErrUnsupported` and context cancellation. gRPC status codes remain inspectable with `status.Code` on transport errors.

Compatibility is checked using the embedded canonical descriptor and its independent protocol version, not SDK semver alone. `WithSkipCompatibilityCheck()` or `skipCompatibilityCheck=true` disables preflight at your own risk. Never use it to paper over a missing concurrency capability.
