---
title: Connect to Chronicle
description: Configure endpoints, TLS, OAuth credentials and client ownership in Go.
---

A client shares one gRPC channel across stores and namespaces. Create it with `chronicle.NewClient(options...)` for lazy construction, or `chronicle.Dial(ctx, options...)` to complete authentication, structural compatibility and the first kernel keep-alive before returning.

## Connection strings

The default endpoint is `chronicle://localhost:35000`. Supply another endpoint with `chronicle.WithConnectionString`. Credentials use OAuth client credentials, not HTTP Basic:

```text
chronicle://client-id:percent-encoded-secret@kernel.example:35000
chronicle://[::1]:35000
chronicle://localhost:35000?auth=none
```

Percent-encode reserved characters in credentials. Omitted credentials select the public development client `chronicle-dev-client` / `chronicle-dev-secret`; use explicit credentials or `WithTokenSource` in production. `WithNoAuthentication` and `auth=none` suppress SDK token exchange and authorization metadata. Combining authentication modes fails construction.

`ParseConnectionString` performs no I/O. `String()` and Go formatting redact credentials by displaying only scheme and addresses. Errors never echo the input URI. Option names, boolean values and `auth=none` are case-insensitive, like C#. Percent escapes are decoded without changing literal `+` characters into spaces. Unknown query options and duplicate query keys (including case variants) fail instead of becoming ignored configuration.

The parser recognizes multihost authorities, `chronicle+srv`, API keys, certificate options and balancing options for diagnostics. This foundation rejects them at client construction with `ErrUnsupported`. Plaintext `disableTls` is also rejected. Use one endpoint; discovery and supervised reconnection are a later slice.

## TLS and token sources

Certificate validation is **enabled by default**, unlike the C# development default. `WithDevelopmentDefaults()` explicitly permits a self-signed local kernel certificate. `skipTlsValidation=true` is an explicit URI alternative. Neither overrides an explicit `WithTLS` policy; `skipTlsValidation=false` also wins over development defaults.

`WithTLS(*tls.Config)` clones the configuration and its root pool for both OAuth HTTPS and gRPC. Set `RootCAs` with `x509.NewCertPool`/`AppendCertsFromPEM`; load client certificates with `tls.LoadX509KeyPair`. TLS 1.2 is the minimum. Treat supplied certificates, private keys and TLS callbacks as immutable after configuration. Password-protected certificate files are unsupported; URI certificate options never silently succeed.

`WithTokenSource(source)` accepts a concurrent, context-aware `Token(ctx) (chronicle.Token, error)` implementation. You own that source; the client never closes it. Nonzero expiration must be in the future. Tokens format as redacted, but their `AccessToken` field is sensitive.

Built-in OAuth sends a form POST to the selected kernel's `/connect/token`, caches tokens, refreshes within one minute of expiry and serializes refreshes. A failed refresh can serve a still-valid token; failed exchanges are throttled for five seconds. The default lifetime without `expires_in` is 3600 seconds. Expired credentials, redirects and malformed responses fail closed. Token requests have a five-second budget. An `Unauthenticated` unary or stream failure invalidates the cached token, including its failed-refresh fallback. The next operation or explicit `Connect` obtains a fresh token; no operation is automatically retried. External sources can implement the concurrency-safe, nonblocking `TokenInvalidator.Invalidate()` contract for the same notification.

## Lifecycle and cancellation

- Client, store and sequence handles support concurrent calls. Caller event values must not be mutated during serialization.
- `WithConnectTimeout` defaults to five seconds and never extends a shorter caller deadline.
- Unary operations use the caller's deadline, not a hidden short timeout. Always supply a bounded context for external I/O.
- Concurrent `Connect` and same-store creation calls share an attempt. Its initiating context bounds that attempt; other waiters can cancel independently. Failed store registration is not cached permanently.
- `EventStore(ctx, name, WithNamespace("tenant"))` caches by both store and namespace. The default namespace is `Default`, matching C# and the kernel (case matters). Namespace selection is not authorization.
- `Close()` is idempotent. It cancels admitted RPCs and joins the keep-alive worker. Later operations return `ErrClosed`. A custom token source that ignores cancellation can prevent shutdown from completing.
- `WithGRPCConnection(conn)` borrows a channel; you own its security, retry policy and eventual close. The SDK still applies its own admission, metadata and compatibility checks. You must explicitly select `WithConnectionString` as the OAuth authority, `WithTokenSource`, or `WithNoAuthentication`; the SDK never guesses a localhost authority for a borrowed channel. Use `WithNoAuthentication` if the supplied channel owns credentials. Do not enable application-level append retries on a borrowed channel.

The foundation acknowledges keep-alives but has no missing-heartbeat watchdog, graceful drain, cluster balancing, background registration retry or automatic connection-generation supervisor. When the stream ends, existing handles fail until an explicit successful `Connect`; supervised recovery is a later slice.

## Diagnose connection failures

Check kernel health with `curl` against `/health`, then inspect the returned error. Use `errors.As` for `CompatibilityError` and `EnvelopeError`; use `errors.Is` for `ErrClosed`, `ErrInvalidConfiguration`, `ErrUnsupported` and context cancellation. gRPC status codes remain inspectable with `status.Code` on transport errors.

Compatibility is checked using the embedded canonical descriptor and its independent protocol version, not SDK semver alone. `WithSkipCompatibilityCheck()` or `skipCompatibilityCheck=true` disables preflight at your own risk. Never use it to paper over a missing concurrency capability.
