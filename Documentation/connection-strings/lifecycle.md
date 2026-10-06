---
title: Connection lifecycle and readiness
description: Understand automatic recovery, registration outcomes and bounded Go client shutdown.
---

Keep your client and store handles across transient outages. The client replaces failed connection generations, refreshes endpoint discovery and OAuth, and replays the frozen registrations before admitting writes through those handles. It does not replay application operations.

## Connection versus registration

`NewClient` validates configuration without network I/O. `Connect(ctx)` and `Dial(ctx, ...)` wait for compatibility preflight and the first acknowledged kernel heartbeat. Construction of a gRPC channel is not readiness.

`EventStore(ctx, name, ...)` ensures the store and namespace and registers event types before returning a cached handle. Cache keys include both store and namespace. Definitions are shared only within the same logical store and generation; namespace barriers remain separate. A failed creation can be retried without permanently poisoning its cache entry.

`Ready(ctx)` waits for connection health and required registration of the cached handles captured when called. A concurrently created store has its own barrier. `store.WaitForRegistration(ctx)` returns a `RegistrationOutcome` and an error, joining a pass or starting a new one after failure. Registration means sent, not that observers are subscribed and caught up; see [Wait for observers before the first append](../reactors.md#wait-for-observers-before-the-first-append) for a known upstream hazard. Registration success refers to that generation, not a promise that the connection cannot subsequently fail.

An outcome contains:

- `Generation` and `Pass` (the pass number is local to that namespace barrier).
- `HasRun`, `Attempts`, and `IsSuccess()`; a zero outcome is not success.
- `Artifacts`: acknowledged store, namespace and event-type stages, including the failed stage.
- `Failure` and `RetryPending`; `RegistrationError` unwraps the underlying cause.

Event-type registration acknowledges a batch, not separate server verdicts for individual definitions. This surface currently covers event types only. It makes no readiness claim for unimplemented reactors, reducers or other artifacts. Registry additions after `NewClient` are still excluded from its frozen snapshot.

## Connection hooks

Use `WithOnConnected(ConnectionHook)` and `WithOnDisconnected(ConnectionHook)` to
observe application connection transitions. These are notifications, not the SDK's
internal registration mechanism. A hook cannot reject a connection, and its
completion is never a readiness barrier.

For example, this function-body excerpt uses `context`, `fmt` and
`chronicle "github.com/cratis/chronicle.go"` imports:

```go
client, err := chronicle.NewClient(chronicle.WithOnConnected(func(_ context.Context, event chronicle.ConnectionEvent) {
    fmt.Printf("connected generation %d\n", event.Generation)
}))
if err != nil {
    panic(err)
}
defer func() { _ = client.Close() }()
// Call client.Connect(ctx) to start supervision and receive notifications.
```

| Contract | Behavior |
| --- | --- |
| Registration | Options accumulate in declaration order without deduplication. Nil hooks fail construction with `ErrInvalidConfiguration` before I/O. Hook collections freeze at `CaptureClient` and apply to every owned or borrowed generation. |
| Connected | The generation reached authenticated protocol readiness, the same point `Connect` returns. This does not prove artifact registration or observer attachment; use `WaitForRegistration` and `Ready` for their documented barriers. |
| Event | `ConnectionEvent` carries `Generation` (matching `RegistrationOutcome.Generation`), `ConnectionID`, selected `Address`, and `Err`. Connected has no error; Disconnected carries the termination cause. |
| Dispatch | One client-owned dispatcher orders Connected(N), Disconnected(N), Connected(N+1). Each event's hooks run concurrently and are joined before the next event. Hooks never run on the caller goroutine or under SDK locks. |
| Progress | Supervision, reconnect, RPCs and `Ready` never wait for hooks. A hook may call `Ready`, `EventStore` or `WaitForRegistration` without deadlocking dispatch. |
| Coalescing | If a generation ends before its Connected hooks start, both notifications are skipped. Generation gaps expose this; at most two events are pending. Every delivered Connected has exactly one Disconnected. |
| Cancellation | Connected uses the generation context, canceled on loss or client close. Disconnected uses the client lifetime context, which may already be canceled. |
| Failure | Panics are contained and their values discarded. `WithLogger` receives fixed operation `client`, stage `connected`/`disconnected`, category `panic` diagnostics without panic text. |
| Shutdown | Close and Shutdown pair delivered Connected events with Disconnected carrying `ErrClosed`. Both join running hooks. Hooks must not call Close or Shutdown themselves; use `CloseContext` to bound waiting for a hook that ignores cancellation. |
| Borrowed channel | Events describe logical sessions, not physical channel state; `Address` is empty. |
| Skipped session | With `WithSkipKeepAlive`, `ConnectionID` is empty and Disconnected occurs only on Close/Shutdown. There is no session stream to detect loss or drive reconnect. |

See the executable `ExampleWithOnConnected` and `ExampleWithOnDisconnected` in
[the connection examples](https://github.com/Cratis/Chronicle.Go/blob/main/example_connection_test.go).

## Evict cached event stores

Call `client.EvictEventStores()` to clear all store/namespace lookup entries and
remove them from subsequent automatic registration passes. It returns an `error`
for an unprepared or closed client, using the existing `ClientStateError` and
`ErrNotPrepared`/`ErrClosed` identities. An empty prepared cache succeeds, including
before connection. There is no context argument: eviction performs no I/O,
callbacks, cancellation or joining.

The next `EventStore` lookup returns a different `*EventStore` for the same
coordinates. Existing handles still borrow the client and share its retained
coordinate resources with the new facade. Sequences, append subscriptions,
observer runs, unregister decisions and local reducer feeds are not duplicated
or closed. An already-resolved provider scope keeps its facade; a new scope uses
the new cache entry. Use client shutdown, not eviction, to stop owned work.

Eviction is **membership removal, not revocation**:

- Registration started by an acquisition, `Ready` or reconnect snapshot before
  eviction may finish afterward. An older completion cannot replace a newer
  cached facade.
- Later `Ready` and automatic reconnect/retry snapshots omit detached handles.
  `Ready` can therefore succeed with an empty cache without registering a
  retained handle's namespace.
- Explicit operations on an old handle can register the current generation;
  this does not put that handle back into the cache. Existing watches keep their
  independent lifetimes. A watch's own explicit registration can make a namespace
  ready without restoring cache membership.
- An already-ready identity manager stays ready on eviction alone. After a
  generation change, if its namespace has not been registered, rename returns
  `registration_not_ready` without starting registration. Explicitly call the
  retained store's `WaitForRegistration(ctx)` before retrying.

Cumulative projection definitions, uncertainty fences and decision evidence are
unchanged by eviction. Later projection publication still updates new
`ReadModels()` selections through detached handles and invalidates old guards;
previously issued readers keep their immutable snapshots.

Eviction does not delete server data or promise memory reclamation. The client
retains one resource owner per distinct store/namespace until shutdown, not one
per eviction. It does not promise uninterrupted delivery across generation loss:
interrupted watches still require explicit rewatching and reconciliation.

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

The client closes only its own gRPC and HTTP transports. Borrowed gRPC channels, custom load balancers and external token sources remain caller-owned, including after failure, replacement and shutdown. The caller is responsible for disabling ambiguous write retries on a borrowed channel.

[Configure endpoints and authentication](index.md) for the discovery, balancing and TLS options.
