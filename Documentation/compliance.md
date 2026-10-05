---
title: Protect and erase personal data
description: Declare PII and confidentiality metadata, release protected values and manage subject-key erasure.
---

Mark personal data with `chronicle:"pii"` and use
`store.Compliance().ErasePII(ctx, subject)` to destroy its readability without
removing events. The kernel owns encryption, erasure fences and replacement keys;
the Go SDK never holds keys or performs local cryptography.

**Runtime limits:** the pinned 19.29.4 MongoDB kernel cannot provision global
confidentiality keys, and protected numeric scalars release with the wrong JSON
kind. These are [tracked compatibility limits](parity.md#compliance-and-confidentiality),
not successful encryption/release claims. String PII, nested string values,
subject/namespace confidentiality and the subject-key lifecycle have kernel tests.

## Declare classifications

Import `events`, `readmodels` and `compliance` from
`github.com/cratis/chronicle.go`. Register events and models before constructing
the client, as with other declarations. Field tags need no additional option.

```go
type Email string

type PersonRegistered struct {
    Owner string `json:"owner" chronicle:"subject"`
    Email Email `json:"email" chronicle:"pii;compliance-details(value=\"contact\")"`
    OperationalValue string `json:"operationalValue" chronicle:"encrypted"`
}
```

`Owner` identifies whose data this event carries. It is not the actor, projection
key or event source. Omit `subject` when the event source is already the person.
Use an opaque surrogate identity; never classify `events.SourceID` as protected.
Subjects beginning with `$chronicle-encrypted-value$` belong to the kernel's
confidentiality key space and fail admission with `compliance.InvalidSubjectError`.
This includes explicit, tagged/resolved and source-fallback subjects in single
appends, batches, units of work and reactor effects, plus seed source IDs. The
check also covers unclassified events whose subjects may reach protected models.

| Declaration | Meaning |
| --- | --- |
| `pii` | Erasable personal data; always subject-scoped |
| `compliance-details(value="reason")` | Rationale for PII; no classification by itself |
| `encrypted` | Non-erasable subject-scoped confidentiality |
| `encrypted(scope=namespace,details="purpose")` | Confidentiality shared within a store/namespace |
| `encrypted(scope=global,details="purpose")` | Installation-wide confidentiality; blocked on the pinned MongoDB kernel |
| `subject` | Explicit owner for an event or model |

PII and confidentiality emit separate `compliance` and `security` schema arrays.
They cannot classify the same value, even through inherited/type metadata. Erasing
a person's PII must not destroy that person's operational confidentiality key.
`compliance-details` is not an encryption purpose; use `encrypted(details=...)`.
Details become persisted schema metadata: never put personal values or secrets
in them.

For type-wide classification, pass `events.WithProtection(...)` or
`readmodels.WithProtection(...)`. This excerpt uses the types above and assumes
`registry` is your registry:

```go
event, err := chronicle.RegisterEvent[PersonRegistered](registry,
    events.WithProtection(
        compliance.For[Email](compliance.Classification{PII: true, Details: "contact"}),
    ),
)
```

The registered event must actually contain `Email`; unreachable type declarations
fail rather than silently doing nothing.

- Composite object classification descends to its leaves without changing the
  object shape. Classified concepts retain their representation and metadata in
  pointers and collection items. An explicitly classified collection remains
  coarse container protection, matching C#. Protection beneath an unprotected
  map, and classified collection-valued array elements, fail registration with
  `declarations.DeclarationError`: the pinned kernel cannot apply that metadata.
  Protect the whole map/outer collection property explicitly, or use supported
  scalar items and declared object members. Metadata is never silently moved.
- Member metadata takes precedence over declaring-type and value-type metadata.
  PII/confidentiality conflicts still fail; precedence cannot bypass that guard.
  Use `DetailsSet: true` to explicitly override an inherited rationale with an
  empty string in a typed declaration.
- `compliance.Property(path, classification)` is the explicit serialized-path
  form. Existing `readmodels.WithPII(paths...)` uses this same pipeline. Paths
  follow the selected naming plan; duplicates and unknown paths fail.
- `compliance.Using(provider)` admits deterministic, I/O-free metadata providers
  instead of assembly discovery. Providers are evaluated during compilation,
  never per read. Registries remain independent; there is no global type cache.
- Unsupported serializers, polymorphic/interface payloads and declarations on
  ignored fields still fail. Nested ownership cannot be inferred from an inner
  `subject` tag: use one subject per event and preserve read-model lineage.

## Erase and deliberately reauthorize

Given a connected `store`, a bounded `ctx` and the person's subject string:

```go
if err := store.Compliance().ErasePII(ctx, subject); err != nil {
    return err
}
```

`DeleteEncryptionKeyFor` is the C#-named equivalent. Erasure reaches **every event
store in this handle's namespace**, including stores holding copies of the key.
Other namespaces are untouched. Repeat explicitly per tenant when the request
spans tenants; finishing only some namespaces is an incomplete erasure.

The kernel fences future key creation before deletion and evicts cached keys.
Reads of shredded string values return empty strings; coarse protected containers
return empty containers. New PII appends fail. Always check both the append
operation error and `result.Err()`; do not treat an unknown append outcome as a
safe retry or infer a typed kernel exception from its text.

Only when the application deliberately starts a new lifecycle:

```go
if err := store.Compliance().AllowNewEncryptionKeyFor(ctx, subject); err != nil {
    return err
}
```

This permits future writes; it cannot recover erased ciphertext. It is never an
automatic recovery step after failed erasure. Confidentiality-key identifiers
are rejected by both lifecycle operations. The server still enforces authorization;
choosing a namespace does not grant access.

A failed lifecycle RPC returns `compliance.ErrLifecycle` with an inspectable
`*compliance.LifecycleError`. Cancellation, gRPC status and `ErrUnsupported` remain
inspectable. Default error text omits the subject and server diagnostics. A failure
can mean partial erasure: do not report completion or log `Cause` indiscriminately.
There is no client retry loop or invented success for an older kernel.

## Release reads without leaking partial results

All model read paths verify release: one-shot reads, hydration sessions,
projection replay, watches, materialized windows and read-model reactor delivery.
For an externally loaded model, use `reader.Release(ctx, value)` or
`reader.ReleaseMany(ctx, values)`. Use `store.ReadModels().Release` for raw sink
documents so stored `__subject` and per-root-property `__subjects` survive.
Only schema-declared fields are sent to the Release RPC; lineage and undeclared
sink bookkeeping stay local and are preserved in the returned document.

Stored lineage wins over a configured or tagged subject, then the model's
case-insensitive Go `ID` property supplies the fallback. A projection `key` marker
alone is not a release subject. Empty/null subjects fall back; a zero UUID retains
its all-zero string, matching C#'s final `ToString` fallback. Numeric subjects
retain exact JSON spelling rather than passing through floating-point decoding.

Namespace/global confidentiality can release without a subject. Present PII or
subject-encrypted values still require an owner, even in a mixed-scope document.
Go deliberately differs from C# best-effort release: failures return
`readmodels.ErrRelease`, with no ciphertext fallback or partially released model,
collection or window. Stored lineage cannot be overwritten by a release response.

Event history is released by the kernel before delivery, as in C#.
`events.Decode[T]` selects the registered codec; it performs no I/O and does not
promise to decrypt arbitrary bytes supplied by a caller. Neither classification
nor erasure removes copies exported outside Chronicle, including logs and backups.

## Run the example

The [compiling example](../examples/compliance/main.go) creates a unique store and
random subject, erases synthetic personal data, verifies confidentiality survives,
checks write refusal and explicitly reauthorizes. It prints no values or keys.
Use a disposable 19.29.4 development kernel with an encryption certificate.
`scripts/configure-compliance-integration.sh CONTAINER` creates an ephemeral test
certificate inside a **fresh disposable** container and restarts it. Never use
that script on an existing data store or production deployment.

```sh
CHRONICLE_INTEGRATION_CONNECTION_STRING=chronicle://localhost:35000 \
  GOWORK=off go run ./examples/compliance
```

Expected output:

```text
Personal data shredded; confidentiality retained; new writes explicitly authorized.
```

The example uses development credentials and disables TLS certificate validation.
Ordinary `go test ./...` remains container-free; kernel tests require the
`integration` build tag and an explicitly supplied connection string.
