---
title: Chronicle for Go
description: "Start with the Go client for Cratis Chronicle: register typed events, append them securely and find guides for projections, reducers and reactors."
---

Chronicle.Go is the Go client SDK for Cratis Chronicle's event store. Chronicle is part of Cratis Platform and can be adopted independently of its other components. Without the client, you manage OAuth, structural compatibility, schemas and RPC envelopes yourself; with it, you register event structs and append facts through context-aware store handles.

## Start here

Requires Go 1.26 or newer. The API may change in v0.x releases. First check the [capability and limits summary](parity.md#v0-capabilities-and-limits), then follow the [first-append source example](clients/go/getting-started.md).

For short-lived producers or on-demand operations, read [one-shot and process ownership](connection-strings/lifecycle.md#short-lived-clients-and-process-ownership). Observer runtimes need normal logical sessions; registration sent does not prove subscription or catch-up.

## Use the foundation

- [Connecting and lifecycle](connection-strings/index.md): endpoints, TLS, OAuth and ownership.
- [Event types](events/event-types.md): stable identities, generations and supported JSON shapes.
- [Serialization](serialization.md): property naming, migration and shared Fundamentals values.
- [Appending events](events/appending-events.md): full results, metadata and concurrency protection.
- [Event enrichment and auditing](events/enrichment.md): declared-field editing and per-call audit snapshots.
- [Identity display names](identities.md): one update with acknowledgment and bounded name observation.
- [Atomic batches](events/batches.md): ordered mixed-source events and independent checks.
- [Reading events](events/reading-events.md): complete history and expectations derived from loaded state.
- [Reactors](reactors.md): plain closures, convention handlers and optional scoped services.
- [Reducers](reducers.md): nullable state folds, convention discovery and passive reads.
- [Read models](read-models/index.md): typed reads, best-effort watches and materialized windows.
- [Definition factories](definition-factories.md): prepare projections, constraints and migrations from configuration.
- [Shared-provider client preparation](facade-composition.md): bind one borrowed client identity before preparing its definitions.
- [Decision reads](read-models/decision-reads.md): admitted optimistic projection guards and owner-controlled commits.
- [Read-model reactors](read-model-reactors.md): Added/Modified/Removed callbacks with scoped effects.
- [Patterns](patterns.md): contextual and usual-action questions with honest empty answers.
- [Parity and limitations](parity.md): executable evidence and deliberate C# differences.
- [Release policy](releases.md): module versioning and compatibility.

Contracts target Chronicle 19.150.1 and real-kernel tests run against 19.150.1-development. The pin is not a supported kernel range. The [capability summary](parity.md#v0-capabilities-and-limits) gives the exact decision-read gate and other conditions. [Release policy](releases.md) distinguishes source, tagged modules and documentation publication; this repository owns the product pages.
