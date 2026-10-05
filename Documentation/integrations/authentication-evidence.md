---
title: Webhook authentication evidence
description: Distinguish capture-source representation, outgoing registration requests and live authentication behavior.
---

Inbound capture sources and outgoing webhooks use different authorization
representations. The client-side observations below help you compare their
contracts; they do not establish that a kernel stores credentials or authenticates
HTTP traffic.

## What is observed

The frozen profile executes public APIs from Chronicle .NET **19.29.4** and
Fundamentals **7.19.6**, on .NET **10.0.12**, Arm64. Its source-authority revision is
`2e31b0dfba489159b3db323238f16d0f277056b4`; the installed package was built from
`ae5e00a8abaa688138b2c2f689e2b4659cccb4fd`. Package, assembly and harness hashes keep
these evidence sources distinct.

| Surface | Actual package observation | Go boundary |
| --- | --- | --- |
| Inbound `WebhookSourceBuilder` | Default null, explicit None, Basic, Bearer, OAuth and last-choice replacement | Authorization authoring is **not implemented** |
| Inbound JSON | Preserve-name and Fundamentals camelCase outer names; fixed inner `type`, `username`, `password`, `token`, `authority`, `clientId`, `clientSecret` | Fixture validation only; no Go authorization decoder |
| Outgoing `Webhooks.Register` | None, Basic and Bearer requests, catalog/selected generations, headers and true/false flags | Existing Go Register requests match observed semantics |
| Outgoing OAuth authoring | No public C# OAuth builder method | Go `WithOAuth` is a converter/contract-backed convenience, not demonstrated C# Register-factory parity |

Inbound null omits the authorization property. Explicit None instead produces
`{"type":"none"}`. The outer source `Type` is numeric; the inner authorization
discriminator is a string. Missing, unknown and case-changed discriminators read
as None in this package, while missing required credential properties fail. Reads
and subsequent writes have independently recorded outcomes. This fallback is an
observation, **not** a recommended fail-open policy for future Go APIs.

C# protobuf-net omits default-true activity/replay flags; Go sends explicit true.
Both preserve explicit false. The tests check presence separately from semantics
rather than requiring identical bytes for the two clients.

## What this does not prove

The harness uses a public borrowed-connection client constructor, an empty type
universe and fixed local collaborators. The real packaged proxy factory and
request marshaller execute, but the recording invoker returns synthetic local
responses. It never connects, registers schemas, contacts an OAuth authority,
sends HTTP or starts a kernel. Only fixed synthetic credentials are captured.
Plaintext representation does not prove encryption or protected persistence.

The existing [Chronicle#4567](https://github.com/Cratis/Chronicle/issues/4567)
kernel limitation remains: selected non-System stores can lack authorization-event
schemas despite successful registration envelopes. The four signature-qualified
integration skips remain skips, not authentication passes. This client-side
capture neither repairs that defect nor adds a live .NET kernel witness.

[Capture declarations](captures.md) still submit CDL only; CDL has no credential
fields, no typed authorization-bearing capture submission RPC is exposed, and
the pinned kernel refuses non-API capture runtime sources. No credential-accepting
Go constructor that drops authorization has been added.

## Evidence and checks

The [capture harness and raw profile](https://github.com/Cratis/Chronicle.Go/tree/develop/captures/testdata/authentication)
contain 55 fixed observations, an independent required-case inventory, complete
provenance and exact permitted-call counts. Any unexpected call invalidates the
whole capture, even if a per-case error handler catches its exception.

`TestAuthenticationPackageEvidence` validates representation and provenance.
The `TestAuthenticationFixtureRejects*` tests plant malformed evidence, including
lost secondary outcomes and invented OAuth Register success.
`TestRegisterMatchesActualAuthenticationCapture` compares existing Go requests
with the six actual packaged C# Register requests. None of these tests claims
credential storage, token acquisition or delivery success.
