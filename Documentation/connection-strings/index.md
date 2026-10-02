---
title: Connect to Chronicle
description: Configure endpoints, TLS, OAuth credentials and client ownership in Go.
---

A client shares one supervised connection generation across stores and namespaces. Create it with `chronicle.NewClient(options...)` for lazy construction, or `chronicle.Dial(ctx, options...)` to complete authentication, structural compatibility and the first kernel keep-alive before returning.

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

`loadBalancer=least-connections` is the default. It probes HTTPS `/connections/count` with a two-second budget per endpoint, randomly breaks ties, and makes a best-effort `/connections/reserve` request. Probes run concurrently over the bounded endpoint set after up to 250ms of jitter. Reservations expire server-side after 30 seconds; there is no release endpoint. `loadBalancer=round-robin` starts at a random offset; `loadBalancer=random` independently selects a candidate. Selection happens per generation, not per append. Single endpoints need no probe.

API keys, URI certificate/password options and plaintext `disableTls` still return `ErrUnsupported`. Configure PEM material through `WithTLS`.

## TLS and token sources

Certificate validation is **enabled by default**, unlike the C# development default. `WithDevelopmentDefaults()` explicitly permits a self-signed local kernel certificate. `skipTlsValidation=true` is an explicit URI alternative. Neither overrides an explicit `WithTLS` policy; `skipTlsValidation=false` also wins over development defaults.

`WithTLS(*tls.Config)` clones the configuration and its root pool for both OAuth HTTPS and gRPC. Set `RootCAs` with `x509.NewCertPool`/`AppendCertsFromPEM`; load client certificates with `tls.LoadX509KeyPair`. TLS 1.2 is the minimum. Treat supplied certificates, private keys and TLS callbacks as immutable after configuration. Password-protected certificate files are unsupported; URI certificate options never silently succeed.

`WithTokenSource(source)` accepts a concurrent, context-aware `Token(ctx) (chronicle.Token, error)` implementation. You own that source; the client never closes it. Nonzero expiration must be in the future. Tokens format as redacted, but their `AccessToken` field is sensitive.

Built-in OAuth sends a form POST to the selected kernel's `/connect/token`, caches tokens, refreshes within one minute of expiry and serializes refreshes. A failed refresh can serve a still-valid token; failed exchanges are throttled for five seconds. The default lifetime without `expires_in` is 3600 seconds. Expired credentials, redirects and malformed responses fail closed. Token requests have a five-second budget. An `Unauthenticated` unary or stream failure invalidates the cached token, including its failed-refresh fallback. The next credential acquisition obtains a fresh token; no application operation is automatically retried. Every replacement generation acquires OAuth credentials from its newly selected endpoint. External sources can implement the concurrency-safe, nonblocking `TokenInvalidator.Invalidate()` contract for the same notification.

## Lifecycle and cancellation

- Client, store and sequence handles support concurrent calls. Caller event values must not be mutated during serialization.
- `WithConnectTimeout` defaults to five seconds and never extends a shorter caller deadline.
- Unary operations use the caller's deadline, not a hidden short timeout. Always supply a bounded context for external I/O.
- Concurrent `Connect` calls share startup. Its initiating context bounds that first attempt; other waiters can cancel independently. Later generations use only client-owned lifetimes. Store registrations are single-flight per generation; failed passes remain inspectable and can recover.
- `EventStore(ctx, name, WithNamespace("tenant"))` caches by both store and namespace. The default namespace is `Default`, matching C# and the kernel (case matters). Namespace selection is not authorization.
- `Close()` cancels and joins owned work. `CloseContext(ctx)` bounds that wait; `Shutdown(ctx)` first drains admitted RPCs. A context error means cleanup is incomplete, not successful. Later operations return `ErrClosed`. A custom source that ignores cancellation can delay actual cleanup.
- `WithGRPCConnection(conn)` borrows a channel; you own its security, retry policy and eventual close. The SDK still applies its own admission, metadata and compatibility checks. Multihost/SRV selection on a borrowed channel is rejected because its owner controls routing. You must explicitly select `WithConnectionString` as the OAuth authority, `WithTokenSource`, or `WithNoAuthentication`; the SDK never guesses a localhost authority for a borrowed channel. Use `WithNoAuthentication` if the supplied channel owns credentials. Do not enable application-level append retries on a borrowed channel.

Transient stream loss or missing heartbeats triggers automatic reconnection and registration replay. Existing handles remain usable after recovery. Authentication rejection and incompatible protocols stop supervision until an explicit `Connect`; readiness never silently bypasses those failures. See [connection lifecycle and readiness](lifecycle.md) for admission, retry and shutdown contracts.

## Diagnose connection failures

Check kernel health with `curl` against `/health`, then inspect the returned error. Use `errors.As` for `CompatibilityError` and `EnvelopeError`; use `errors.Is` for `ErrClosed`, `ErrInvalidConfiguration`, `ErrUnsupported` and context cancellation. gRPC status codes remain inspectable with `status.Code` on transport errors.

Compatibility is checked using the embedded canonical descriptor and its independent protocol version, not SDK semver alone. `WithSkipCompatibilityCheck()` or `skipCompatibilityCheck=true` disables preflight at your own risk. Never use it to paper over a missing concurrency capability.
