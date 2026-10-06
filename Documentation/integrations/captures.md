---
title: Capture declarations
description: Prepare, validate and save explicit capture declarations without claiming automatic activation.
---

Capture declarations describe how changes in external data become events. Go
provides explicit authoring, validation and saving through the kernel's Capture
Declaration Language (CDL) RPCs. **Saving does not start a capture.** There is no
automatic capture discovery, registry category or SDK polling worker.

The 19.29.4-development kernel has capture validation and storage RPCs, but its
engine only supports API sources with root append rules. Webhook/message sources,
map operations, nested/child scopes and expression conditions can be authored;
kernel validation rejects unsupported runtime capabilities explicitly. No
end-to-end external polling/delivery claim is made by the SDK.

## Author a declaration

The complete container-free example is in
[`examples/integrations`](https://github.com/Cratis/Chronicle.Go/blob/develop/examples/integrations/main.go).
Given a registered `events.Type[OrderPlaced]` named `placed`:

```go
definition, err := new(captures.Builder).
    From(captures.API("OrdersApi", "/orders", "5m")).
    Key("id").
    Append(captures.Append(placed, captures.Added(),
        map[string]string{"OrderNumber": "$.number"})).
    Build("ImportOrders")
```

Handle `err`. `Build` requires a name, source, key and a condition for each append.
It creates a UUID and freezes the declaration. Calling `Build` again creates a
new capture identity; retain the definition to update the same saved capture.
The mutable builder belongs to one authoring operation, not concurrent callers.

`captures.Capturer` is the explicit equivalent of C# `ICapturer`: implement
`Define(*captures.Builder) error` and call `captures.Prepare(name, capturer)`.
Preparation invokes it once without discovery, activation scopes or I/O.

## Authoring reference

| Surface | Meaning |
| --- | --- |
| `API(service, route, poll)` | External HTTP service name, optional route and poll interval; kernel units are `s`, `m`, `h`, `d` |
| `Webhook(path, options...)`, `MessageTopic(topic)` | Source declarations; not runnable on the pinned kernel |
| `WithBasicAuth(username, password)`, `WithBearerToken(token)`, `WithOAuth(authority, clientID, clientSecret)` | Inbound webhook authorization authoring only; authorized definitions cannot be submitted |
| `Key(path)` | Stable identity property used for diffing |
| `PropertyChanges(path)`, `AnyOf(paths...)`, `AllOf(paths...)` | Property-change conditions |
| `Transition(path, from, to)` | Transition between string literal values |
| `Added()`, `Removed()` | Item appearance/disappearance |
| `Expression(text)` | Expression condition; authoring only, rejected by the pinned engine |
| `Append(event, condition, assignments)` | Persisted event ID and serialized target-path → CDL expression assignments |
| `Rename`, `Template`, `Translate`, `Split` | Ordered mapping operations |
| `NewScope(mappings, rules...)` | Immutable nested or child scope |
| `Nested(path, scope)`, `Children(path, key, scope)` | Scoped object/identified collection rules |

Source/key/map choices replace previous choices; append and scope declarations
accumulate. Input maps and slices are copied. Repeated assignment keys have normal
Go map replacement semantics; rendering sorts keys for reproducibility.
Conditions and mapping expressions cannot inject new declaration lines. String
values in transitions, translation sources and split separators preserve literal
backslashes, quotes, tabs and line breaks through CDL escaping. A property named
`added` or `removed` remains a property-change condition, not an item lifecycle
condition; only `Added()` and `Removed()` select those lifecycle triggers.

Translation targets are unquoted, nonempty CDL word tokens, such as `open` or
`Open_2`. Spaces, punctuation and dotted paths cannot be represented and fail with
`chronicle.ErrUnsupported`. Map targets and child collection names must be single
identifiers beginning with a lowercase ASCII letter or underscore.

Unlike C# `AppendBuilder<TEvent>`, which uses the CLR simple type name, Go uses the
registered **persisted event ID**. The pinned Screenplay 4.16.0 parser requires a
single identifier starting with an uppercase ASCII letter, followed by word
characters (letters, decimal digits, underscores and other Unicode connectors or
nonspacing marks in the basic multilingual plane). Lowercase-leading, dotted, hyphenated and slash-separated IDs
fail with `chronicle.ErrUnsupported`; they are never silently renamed. Capture
names are single identifiers beginning with an ASCII letter or underscore. Use a
compatible persisted ID from the beginning, not a rename of an already persisted
event solely for a capture.

Model-bound capture tags and automatic activation are not implemented. Configure
outbound API credentials on the referenced external service, not on its capture.

## Author inbound webhook authorization

You can retain inbound Basic, Bearer or OAuth authorization on an immutable source
and definition, but **you cannot submit an authorized capture**. Authoring does not
acquire a token, authenticate a request or prove credential persistence.

This authoring-only excerpt matches the executable `ExampleWithBearerToken`:

```go
definition, err := new(captures.Builder).
    From(captures.Webhook("/synthetic-capture",
        captures.WithBearerToken("synthetic-token"))).
    Key("id").Build("SyntheticCapture")
```

Handle `err`. Omit options for absent authorization. `Source.Authorization()` and
`Definition.Authorization()` return `(SourceAuthorization, bool)`; the Boolean
reports presence, and `Kind()` returns `AuthorizationNone`, `AuthorizationBasic`,
`AuthorizationBearer` or `AuthorizationOAuth`. The zero authorization is None,
but there is no explicit None option. Copies and repeated builds retain the
selected authorization without adding credentials to `Declaration()`.

All credential values must be nonblank after `strings.TrimSpace`; nonblank values
are preserved verbatim. Nil options or more than one authorization option fail at
`Build` with `chronicle.ErrInvalidConfiguration` and a value-free error. Unlike
C#'s last-choice replacement, Go rejects multiple choices so it cannot silently
discard a supplied credential. Source selection through `Builder.From` still
replaces the previous source.

Formatting and structured logging redact `SourceAuthorization`, `Source`,
`Definition` and `Builder`. JSON marshaling and unmarshaling fail with
`chronicle.ErrUnsupported`, including on zero values; they never export an empty
object or silently read unknown authorization as None. If you used JSON for these
configuration values, migrate to explicit authoring; `Declaration()` exports CDL
only, not a complete authorization-bearing definition. `Webhook` is now variadic:
ordinary one-argument calls remain valid, but stored `func(string) Source` function
values must adapt to the new signature.

The [actual-package authentication observations](authentication-evidence.md)
compare the internal converter representation with C# and explain why Go provides
no decoder. They do not establish live authentication support.

## Validate and save

For a definition with source authorization, both `Validate` and `Save` return
`chronicle.ErrUnsupported` before any RPC, even if the context is canceled or its
deadline has expired. No authorized definition is persisted. CDL has no credential
fields; the capture contracts carry CDL only and cannot retain source
authorization. [Chronicle#4591: capture authorization transport and kernel
boundary](https://github.com/Cratis/Chronicle/issues/4591) tracks the missing
submission/enforcement path and unsafe converter fallback. Do not remove
credentials to make submission succeed.

For a definition without source authorization, behavior is unchanged. On a
registered store, first register the referenced external service and events,
then call `store.Captures().Validate(ctx, definition)`. Validation checks both
syntax and runtime support without saving or activating anything.

`store.Captures().Save(ctx, definition)` saves the definition under its UUID. The
kernel can persist a stopped definition **and then return capability messages**.
Go returns `*captures.ValidationError` for those messages even if the outer command
envelope succeeded. Retain `definition.ID()` when correcting or administratively
removing that definition. A successful save is not evidence of activation or
successful polling. Kernel administration through the existing capture contracts
or operator tooling remains separate.

`ValidationError.Error()` is deliberately generic. Its `Messages` may include
mapping literals or other sensitive input; inspect them intentionally rather than
logging every field. Transport and command-envelope failures remain inspectable.
