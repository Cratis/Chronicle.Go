<!-- Copyright (c) Cratis. All rights reserved. -->
<!-- Licensed under the MIT license. See LICENSE file in the project root for full license information. -->

# OpenTelemetry gRPC tracing

This runnable consumer recipe uses `WithGRPCStatsHandler` to attach
`otelgrpc.NewClientHandler` to every SDK-owned connection, including replacements
created by Chronicle's reconnect supervisor. It pins the published Chronicle.Go
`v0.21.0`, otelgrpc `v0.72.0`, and OpenTelemetry SDK/stdout exporter `v1.47.0`.
Dependencies live only in the unpublished `recipes` module, not the root SDK.

## Run

With Go 1.26 and a local development kernel listening with a trusted TLS certificate:

```sh
cd recipes
GOWORK=off go mod download
GOWORK=off go run ./otelgrpc/cmd -address chronicle://localhost:35000
```

The example explicitly selects `WithNoAuthentication` for a local development
kernel. For a secured environment, replace it with the application's credential
configuration. Do not put credentials in the address. Certificate and hostname
verification remain enabled; do not use `WithDevelopmentDefaults` or disable TLS
verification to make this example connect. Trust your development CA through your
normal trust configuration, or supply `WithTLS` with a trusted root pool.

Press Ctrl+C to close the client and print its completed spans as JSON to stdout.
The session stream's span ends only when the session ends. For ordinary RPC spans,
perform your application's operations with a caller-owned context and deadline
between `Connect` and the wait in `Run`.

## Ownership and behavior

`newTracerProvider` creates an application-owned provider with a bounded batch
processor and stdout exporter. The demo samples every span; choose an appropriate
sampling policy and exporter for your workload. `newClient` supplies that provider
and W3C Trace Context propagation explicitly to the upstream stats handler; no
global provider or propagator is changed. Stats handlers are trusted application
callbacks: they must support concurrent calls, return promptly, and preserve
cancellation, deadlines, and context values in `TagRPC`/`TagConn`. Do not call
blocking client lifecycle methods such as `Close` from a callback. This option
does not sandbox arbitrary callback code.

No event payload/message recording or baggage propagation is enabled. Review exported endpoint and error attributes
before sending telemetry to a shared collector.

On shutdown (including startup failure), the application closes the Chronicle
client first, ending its RPCs and streams, then shuts down the provider, which
flushes the processor and shuts down the exporter. The final flush uses a fresh
five-second cleanup budget even if the application context is already canceled.
The SDK never owns or shuts down the provider/exporter. Do not reverse this order
or export RPCs ending during client shutdown may be lost.

This is connection instrumentation, not domain tracing. It neither adds spans for
individual events nor changes RPC deadlines, TLS, credentials, registration, or
retry rules. Reconnecting a transport does **not** replay a dispatched append; an
ambiguous write remains `OutcomeUnknownError` and needs application reconciliation.
No borrowed `ClientConn` is used: instrument a borrowed connection yourself at its
construction site because the SDK cannot retrofit its handlers.

## Verify without a collector or kernel

```sh
cd recipes
GOWORK=off go build ./...
GOWORK=off go vet ./...
GOWORK=off go test ./...
GOWORK=off go test -race ./...
GOWORK=off golangci-lint run --timeout=5m --config ../.golangci.yml ./...
```

The in-memory-exporter tests use a loopback gRPC fixture with a verified TLS
certificate and an explicit trust pool. They check RPC spans before and after a
new connection ID and TCP connection are observed, reject an untrusted server,
verify caller trace context on an interrupted append, assert exactly one server
append call, and check final stream spans and application-owned exporter shutdown.
The fixture skips Chronicle kernel compatibility discovery; these are consumer
instrumentation tests, not proof of compatibility with a running kernel.

The existing CI module matrix discovers `recipes` through
[the module allow-list](../../.github/go-modules.json) and runs its build, vet,
tests, race checks, formatting, tidy, and lint gates separately from the root SDK.
