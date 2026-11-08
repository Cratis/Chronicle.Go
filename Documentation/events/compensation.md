---
title: Compensate for recorded events
description: Append a compensating event that corrects an earlier event without changing it, keeping the event log append-only and the history auditable.
---

A compensating event records a correction as a new fact. The original event stays
exactly as it was, and the new event carries the original's sequence number so
observers can tell what it corrects. Use it for ordinary domain corrections.
Use [revision or redaction](history-revision.md) only for administrative repair.

## Declare the compensating type

Declare which event type a type compensates with `events.WithCompensationFor`.
The target must be registered in the same store.

```go
original, err := events.Define[OrderPlaced]()
if err != nil {
    return err
}
cancelled, err := events.Define[OrderPlacementRetracted](events.WithCompensationFor(original))
if err != nil {
    return err
}
```

## Append the compensation

```go
result, err := store.EventLog().Compensate(ctx, position, OrderPlacementRetracted{Reason: "duplicate"})
if err != nil {
    return err // refused before dispatch, or the outcome is unknown
}
if err := result.Err(); err != nil {
    return err // for example eventsequences.CompensatedEventNotFound
}
```

`Compensate` works like an append, except the event source, stream and subject come
from the compensated event. `WithRoute`, `WithScope`, `WithSubject` and
`WithNamedTags` are therefore refused with `ErrInvalidConfiguration`;
`WithCorrelation`, `WithTags` and `WithOccurred` apply.

- A type that is not declared with `WithCompensationFor` is refused before dispatch.
- The kernel rejects a missing target (`CompensatedEventNotFound`) and a type that
  does not compensate the target's type (`EventTypeDoesNotCompensateEventType`). Both
  come back as a `Rejected` result with a nil error; `result.Err()` promotes them.
- A failure after dispatch is an `OutcomeUnknownError`. Do not retry blindly: a retry
  can append a second compensation.
- The call is not retried, does not emit local append notifications and does not
  join a unit of work.
- It needs Chronicle 19.125.0 or later.

## Read the link

`events.Context.CompensationFor` holds the position of the corrected event, or nil
for an ordinary event. The wire format uses a plain number where zero means none, so
a compensation of the event at position zero reads as nil.
