---
title: Connection lifecycle and readiness
description: Understand automatic recovery, registration outcomes and bounded Go client shutdown.
---

Keep your client and store handles across transient outages. The client replaces failed connection generations, refreshes endpoint discovery and OAuth, and replays the frozen registrations before admitting writes through those handles. It does not replay application operations.

## Connection versus registration

`NewClient` validates configuration without network I/O. `Connect(ctx)` and `Dial(ctx, ...)` wait for compatibility preflight and the first acknowledged kernel heartbeat. Construction of a gRPC channel is not readiness.

`EventStore(ctx, name, ...)` ensures the store and namespace and registers event types before returning a cached handle. Cache keys include both store and namespace. Definitions are shared only within the same logical store and generation; namespace barriers remain separate. A failed creation can be retried without permanently poisoning its cache entry.

`Ready(ctx)` waits for connection health and required registration of all handles known when called. A concurrently created store has its own barrier. `store.WaitForRegistration(ctx)` returns a `RegistrationOutcome` and an error, joining a pass or starting a new one after failure. Registration success refers to that generation, not a promise that the connection cannot subsequently fail.

An outcome contains:

- `Generation` and `Pass` (the pass number is local to that namespace barrier).
- `HasRun`, `Attempts`, and `IsSuccess()`; a zero outcome is not success.
- `Artifacts`: acknowledged store, namespace and event-type stages, including the failed stage.
- `Failure` and `RetryPending`; `RegistrationError` unwraps the underlying cause.

Event-type registration acknowledges a batch, not separate server verdicts for individual definitions. This surface currently covers event types only. It makes no readiness claim for unimplemented reactors, reducers or other artifacts. Registry additions after `NewClient` are still excluded from its frozen snapshot.

## Registration retries

`WithRegistrationRetry(RegistrationRetry{...})` replaces the complete policy. Defaults match C#'s five attempts and jittered exponential delay from two to thirty seconds. Go also bounds each attempt to thirty seconds. All fields must be positive, `InitialDelay` cannot exceed `MaximumDelay`, and `MaxAttempts` must be between one and 100.

Only idempotent registration stages retry, for unavailable/deadline/resource-exhausted/aborted transport failures. Caller cancellation stops the current pass and its waiter; it does not cancel the client. After exhaustion, a single generation-owned worker retries transient failures after `MaximumDelay`. Deterministic schema, authorization and validation failures are reported without a background retry loop. A later explicit registration wait can retry them. Previously acknowledged store-wide stages are not resent within that generation.

## Automatic recovery

One supervisor owns generation replacement. A generation has a fresh connection ID, compatibility verdict, keep-alive stream and registration barriers. On failure it cancels and joins the old receiver, registration worker and admitted RPCs before creating the replacement. Cached handles route to the new generation without retaining the old transport.

The heartbeat silence threshold defaults to five seconds; configure a positive duration with `WithKeepAliveTimeout`. Owned gRPC channels also send HTTP/2 pings after 60 seconds of inactivity with a 30-second timeout, matching C#. Transport pings do not replace the application heartbeat.

Transient reconnection uses cancellation-aware exponential backoff, nominally one to thirty seconds with downward jitter. There is no fixed attempt count: the single supervisor continues until recovery, a terminal failure or shutdown. Each attempt has the five-second default `WithConnectTimeout` budget, covering discovery, selection, authentication and handshake. Concurrent callers do not create competing retry loops.

Bad OAuth credentials, rejected authentication/authorization, unsupported compatibility RPCs, incompatible descriptors and malformed keep-alive identities fail closed. `Ready` returns the terminal cause; an explicit subsequent `Connect` starts a new attempt. This deliberately avoids C#'s blanket transport retries and its permissive compatibility failure fallback.

During an outage, ordinary handle operations fail before dispatch. During replay they wait for their registration barrier within the caller's context. An already dispatched append interrupted by generation loss returns `OutcomeUnknownError`; it is never automatically retried. The kernel may have committed it even though the response was lost.

## Shutdown and ownership

`Shutdown(ctx)` closes admission, lets admitted RPCs finish while the deadline permits, then cancels supervision/streams and joins owned work. It cannot drain an application operation that has not yet entered an RPC. For example, shutdown between a sequence's tail read and append rejects the later append before dispatch.

`Close()` immediately cancels and joins without a deadline. It retains its slice-1 signature. `CloseContext(ctx)` performs the same cancellation but bounds how long the caller waits. Repeated calls are safe; a later close can join cleanup after an earlier deadline expired.

A deadline or cancellation error means cleanup is still incomplete. Cleanup continues, and no new work is admitted. Go cannot forcibly terminate a token-source or resolver callback that ignores its context; honoring cancellation is part of those extension contracts. The SDK never holds its internal state locks while invoking them.

The client closes only its own gRPC and HTTP transports. Borrowed gRPC channels and external token sources remain caller-owned, including after failure, replacement and shutdown. The caller is responsible for disabling ambiguous write retries on a borrowed channel.

[Configure endpoints and authentication](index.md) for the discovery, balancing and TLS options.
