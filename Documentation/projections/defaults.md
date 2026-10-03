---
title: Projection initial values and labels
description: Supply initial read-model state and label the projection artifact.
---

Use initial values for state that an event mapping does not supply. Labels describe
an artifact for organization; they **never select or filter events**.

## Declare initial state

This declaration comes from the [compiling stock example](../../examples/projections/defaults.go).
It uses `chronicle`, `projections`, and `readmodels`. Call `stockDeclarations()`
before constructing your client; register and read it as shown in the
[projection guide](index.md#run-and-read-the-model).

```go
type StockRegistered struct {
    Name string `json:"name"`
}

type Stock struct {
    ID string `json:"id"`
    Name string `json:"name" chronicle:"set(StockRegistered)"`
    Available int32 `json:"available"`
    Locations []string `json:"locations"`
    Note *string `json:"note"`
}

func stockDeclarations() (*chronicle.Registry, readmodels.Model[Stock], error) {
    registry := chronicle.NewRegistry()
    registered, err := chronicle.RegisterEvent[StockRegistered](registry)
    if err != nil {
        return nil, readmodels.Model[Stock]{}, err
    }
    model, err := chronicle.RegisterReadModel[Stock](registry)
    if err != nil {
        return nil, model, err
    }
    declaration := projections.ModelBound(model,
        projections.FromEvent(registered),
        projections.WithInitialValues(Stock{Available: 10, Locations: []string{}}),
        projections.WithInitialValue(projections.Path[Stock, *string]("note"), nil),
        projections.WithLabels("inventory", "warehouse", "inventory"))
    return registry, model, registry.AddProjection(declaration)
}
```

Both `ModelBound` and `NewBuilder` accept these options. Initial values use the
registered model codec, including concepts, nested objects, explicit JSON names,
and the client's frozen naming policy. Paths use declaration-time serialized names,
just like mapping paths. Preparation copies the serialized state; later mutation
of a slice, map, pointer, builder, registry, or returned protobuf cannot change the
submitted bytes or reconnect definition. Do not mutate inputs while preparing them.

| Option | Contract |
| --- | --- |
| `WithInitialValues(modelValue)` | Typed whole model; repeated calls replace the state. No option means `{}` |
| `WithInitialValue(Path[M,V](path), value)` | Typed scalar path, including concepts and nullable scalar pointers; no collection traversal. Duplicate writes or overlapping scalar paths fail |
| `WithLabels(labels...)` | Copied artifact metadata; calls accumulate, exact duplicates keep their first occurrence. Case and nonblank whitespace remain significant |

Whole-model serialization preserves zero and false unless `omitempty`/`omitzero`
requests omission. A non-nil empty slice/map supplies `[]`/`{}`; nil properties
are omitted. The scalar-path option supplies an explicit zero despite omission
flags, or explicit JSON `null` for a nil scalar pointer. Null roots, arbitrary JSON,
foreign model types, collection-null initializers, invalid codecs, and nonfinite
numbers are not accepted. Invalid declarations fail `Build`/`NewClient` before I/O.
Labels must be nonblank UTF-8 without control characters; they are not trimmed.

## Materialization and protection boundaries

Registration alone creates no model instance. The kernel applies initial state
when an event materializes it; subsequent mappings update existing state. Removal
removes the instance, and a later creating event initializes it again. Do not use
registration or successful append as a sink-completion signal.

Initializer state is definition metadata, not an appended event. It does not pass
through append-time encryption. Go therefore rejects **any supplied protected root**,
including explicit null, when its schema carries PII or confidentiality metadata.
Omitting that root is allowed. This conservative boundary also covers protected
nested values and collections; it does not promise encrypted default support.

The pinned kernel's all-instance replay used by `ReadModelScenario` omits initial
state. Scenarios with nonempty projection defaults fail with
`ErrFidelityUnavailable`; use production materialized reads instead. Scenario
support remains a [#38 follow-up](https://github.com/Cratis/Chronicle.Go/issues/38).
Explicit observer replay and materialized read-back are separate from that scenario
endpoint. Initial-value authoring does not promise history revision/redaction rewind
semantics or automatically trigger replay when only defaults change.

C# uses a whole-model callback; Go takes a typed value and snapshots it during
preparation. Model-bound options and scalar-path presence are Go additions.
Initial-state JSON object keys are sorted for deterministic identity rather than
following C# declaration order. See the [parity map](../parity.md#projection-initial-values-and-artifact-labels)
for the source contract and evidence boundaries.
