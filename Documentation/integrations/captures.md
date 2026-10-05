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
| `Webhook(path)`, `MessageTopic(topic)` | Source declarations; not runnable on the pinned kernel |
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

Webhook-source authorization is **not implemented**: CDL has no authorization
fields and the kernel exposes no typed capture-definition submission RPC. Go does
not discard credentials or embed them in capture text. Configure outbound API
credentials on the referenced external service instead. Model-bound capture tags
and automatic activation are also not implemented. [Actual-package authentication
observations](authentication-evidence.md) document the C# inbound representation;
they do not add a Go authorization API or prove authentication enforcement.

## Validate and save

On a registered store, first register the referenced external service and events,
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
