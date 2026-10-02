---
title: Chronicle for Go
description: Register typed events and append them securely to Cratis Chronicle from Go.
---

Chronicle.Go is the Go client SDK for Cratis Chronicle's event store. Without the client, you manage OAuth, structural compatibility, schemas and RPC envelopes yourself; with it, you register event structs and append facts through context-aware store handles.

## Start here

[Append your first event](clients/go/getting-started.md) using a local development kernel. Requires Go 1.26 or newer. The API remains experimental in the v0.x release series.

## Use the foundation

- [Connecting and lifecycle](connection-strings/index.md): endpoints, TLS, OAuth and ownership.
- [Event types](events/event-types.md): stable identities, generations and supported JSON shapes.
- [Appending events](events/appending-events.md): full results, metadata and concurrency protection.
- [Parity and limitations](parity.md): executable evidence and deliberate C# differences.
- [Release policy](releases.md): module versioning and experimental compatibility.

Contracts target Chronicle 19.29.4; kernel-backed tests run against 19.29.4-development. Batches, high-level reads, observers, projections and supervised recovery are not part of this foundation. Inclusion in the central Cratis documentation site remains a separate site integration.
