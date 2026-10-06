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
| Inbound `WebhookSourceBuilder` | Default null, explicit None, Basic, Bearer, OAuth and last-choice replacement | **Partial**: Go authors Basic/Bearer/OAuth and retains absent authorization; explicit None is not authorable, blanks and multiple choices are rejected |
| Inbound JSON | Preserve-name and Fundamentals camelCase outer names; fixed inner `type`, `username`, `password`, `token`, `authority`, `clientId`, `clientSecret` | Internal converter bytes match both policies; public JSON import/export fails closed, with no decoder |
| Outgoing `Webhooks.Register` | None, Basic and Bearer requests, catalog/selected generations, headers and true/false flags | Existing Go Register requests match observed semantics |
| Outgoing OAuth authoring | No public C# OAuth builder method | Go `WithOAuth` is a converter/contract-backed convenience, not demonstrated C# Register-factory parity |

Inbound null omits the authorization property. Explicit None instead produces
`{"type":"none"}`. The outer source `Type` is numeric; the inner authorization
discriminator is a string. Missing, unknown and case-changed discriminators read
as None in this package, while missing required credential properties fail. Reads
and subsequent writes have independently recorded outcomes. This fallback is an
observation, **not** a Go policy. Go rejects all JSON imports with
`chronicle.ErrUnsupported` rather than dropping credentials through a None
fallback. Public JSON exports also fail with that error; only internal golden
tests encode authorization, with no public credential-export API.

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

Kernel 19.29.4 registered authorization-event schemas only in the System store
([Chronicle#4567](https://github.com/Cratis/Chronicle/issues/4567)). Kernel 19.32.3
fixes that, and the four registration integration tests now require the
authorization event in the selected store. This client-side capture adds no live
.NET kernel witness.

[Capture declarations](captures.md) still submit CDL only; CDL has no credential
fields, no typed authorization-bearing capture submission RPC is exposed, and
the pinned kernel refuses non-API capture runtime sources. Go retains authored
credentials on source/definition copies, but `Validate` and `Save` refuse them
with `chronicle.ErrUnsupported` before dispatch, including with canceled or
expired contexts. [Chronicle#4591: capture authorization transport and kernel
boundary](https://github.com/Cratis/Chronicle/issues/4591) tracks the contracts,
kernel authorization handling and converter None fallback. No authenticated
capture submission, credential storage, OAuth acquisition or delivery claim is
made.

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

`TestSourceAuthorizationMatchesActualPackage` checks all four converter arms
under both naming policies byte-for-byte. `TestWebhookSourceAuthorizationMatchesActualSerialize`
checks single-option nested bytes and absent defaults; explicit None is compared
only as a zero value. Its replacement cases are deliberately **Go-refused**, not
claimed as C# last-wins parity. `TestSourceAuthorizationHasNoDecoder` refuses all
12 deserialize inputs under both policies. The fixture loader validates hashes
before any comparison.

`TestCaptureAuthorizationRejectsInvalidOptions`, `TestCaptureDeclarationNeverCarriesCredentials`,
`TestCaptureAuthorizationRedacted` and `TestCaptureAuthorizationSurvivesCopies`
cover configuration, CDL exclusion, diagnostics and immutable ownership.
`TestCaptureAuthorizationSubmissionRefusedBeforeDispatch` proves zero RPC/stream
calls for Basic/Bearer/OAuth with active, canceled and expired contexts; the
no-authorization control still submits identical CDL. The kernel witness
`TestKernelCaptureAuthorizationSubmissionBoundary` contrasts an unprotected
webhook saved as stopped with a locally refused bearer definition whose ID is
absent from `GetCaptures`. It proves refusal without remote mutation, not kernel
authentication enforcement.
