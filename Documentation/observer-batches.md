---
title: Handle events in batches
description: Receive pages of events in one reactor or reducer method, declare its event types with observation.Observes, and report failures or deferrals per partition.
---

Chronicle delivers events to reactors and reducers in **pages**. A page holds the
events of one or more partitions (event sources), in sequence order, on every
path: live, catch-up, replay and retry. The Go client always asks for pages.
A kernel that predates pages keeps sending one partition at a time, and
everything below still works.

Your existing per-event methods need no change. The client calls them once per
event, partition by partition. A **batch method** is called once per page
instead, with every event of the types it observes. Use it when the work is
cheaper in bulk: one request to a search index instead of one per event, or one
query that loads supporting data for many events.

## Declare a batch method on a reactor

A batch method takes an optional `context.Context`, then
`*observation.EventBatch`, then optionally `observation.BatchContext` and
services. It returns nothing or `error`. Go has no attributes, so you declare
the event types it observes when you register the reactor, with
`reactors.WithBatchMethod` and one `observation.Observes[E]()` per type. This is
the Go form of C# `[Observes<TEvent>]`. Inside the method, switch on the event
type. Batch events are values (`E`), not pointers.

The following excerpt comes from the
[compiling example](https://github.com/Cratis/Chronicle.Go/blob/main/example_observer_batches_test.go):

```go
// Users indexes users with one bulk request per page.
type Users struct{ index SearchIndex }

func (u *Users) Index(ctx context.Context, batch *observation.EventBatch, _ observation.BatchContext) error {
    var operations []IndexOperation
    for _, item := range batch.All() {
        switch event := item.Event.(type) {
        case UserRegistered:
            operations = append(operations, IndexOperation{User: item.Context.SourceID, Name: event.Name})
        case UserRenamed:
            operations = append(operations, IndexOperation{User: item.Context.SourceID, Name: event.Name})
        case UserRemoved:
            operations = append(operations, IndexOperation{User: item.Context.SourceID, Delete: true})
        }
    }
    rejected, throttled, err := u.index.Bulk(ctx, operations)
    if err != nil {
        return err // Every partition in the batch fails at its first event.
    }
    for _, user := range rejected {
        if err := batch.Fail(user, errors.New("rejected by the index")); err != nil {
            return err
        }
    }
    for _, user := range throttled {
        if err := batch.Defer(user, 30*time.Second); err != nil {
            return err
        }
    }
    return nil
}

func registerUsers(registry *chronicle.Registry, index SearchIndex) error {
    return chronicle.RegisterReactor[*Users](registry, func() *Users { return &Users{index} },
        reactors.WithID("users-index"),
        reactors.WithBatchMethod("Index",
            observation.Observes[UserRegistered](),
            observation.Observes[UserRenamed](),
            observation.Observes[UserRemoved]()))
}
```

For a reactor without an artifact, `reactors.OnBatch(handler, observes...)`
declares a batch callback for `chronicle.RegisterReactorHandlers`.

A reactor can have batch methods and per-event methods side by side. Batch
methods run first, then per-event methods partition by partition. **Order is
kept per method, not across methods**: each method sees its events in sequence
order, and so in order within each partition, but two methods of one reactor
are not ordered relative to each other. Handle side effects that must happen in
a particular order in the same method.

Batch methods cannot return events. Append through the event log you inject.
Middleware surrounds per-event handlers only, as in C#.

## Declare a batch method on a reducer

A reducer uses **either** one batch method **or** per-event folds, never both:
a reducer is one fold over an event source's events in order. The batch method
takes an optional `context.Context`, `*observation.EventBatch`, the current read
model (`M` or `*M`, absent as nil or zero) and optionally
`observation.BatchContext`. It returns `M` or `*M`, optionally followed by
`error`; a nil `*M` deletes the read model. It is called **once per event
source** in the page, and its observed types are the reducer's whole set of
event types.

```go
err = chronicle.RegisterReducer[Accounts](registry, model, nil,
    reducers.WithBatchMethod("Reduce",
        observation.Observes[MoneyDeposited](),
        observation.Observes[MoneyWithdrawn]()),
    reducers.WithMaxConcurrency(4))
```

The event sources of one page fold in parallel, each with its own scope and
artifact. `reducers.WithMaxConcurrency` bounds that; the default is the number
of CPUs, and `1` folds them one after another. The bound applies to per-event
reducers too. `reducers.OnBatch` declares a batch fold callback for
`chronicle.RegisterReducerHandlers`.

## The batch context

`observation.BatchContext` describes the page: the observer, store, namespace
and event sequence, its partitions, how many events it holds, its first and last
sequence numbers, the kernel's deadline (zero when none) and whether the kernel
accepts deferrals (`CanDefer`). Cancellation arrives through the method's
`context.Context`. `Origin` says where the page came from:

| Origin | Meaning |
| --- | --- |
| `OriginLive` | The events were appended now and delivered to an observer that is up to date |
| `OriginCatchUp` | The events were read from storage for an observer that is behind |
| `OriginReplay` | The events were read from storage for an observer that was rewound |
| `OriginRetry` | The events were read from storage to recover a failed or deferred partition |
| `OriginUnknown` | The kernel did not say, or the fold ran locally, for example for a passive read |

Per-event methods cannot take a `BatchContext`.

## Report outcomes

- **Return nil** and every partition in the batch succeeded.
- **Return an error** and every partition in the batch fails at its first event
  in the batch, then goes through the kernel's failed-partition recovery.
- **`Fail(partition, err)`** or **`FailEvent(eventContext, err)`** fails one
  partition from its first event in the batch, or from that event. For a reactor
  the partition's earlier events count as handled. A reducer writes no state for
  a failed partition.
- **`Defer(partition, retryAfter)`** or **`DeferEvent(eventContext, retryAfter)`**
  delivers the partition again after the delay. A deferral is not a failure and
  does not count towards quarantine, so use it when a downstream system asks you
  to back off. A reducer writes no state for a deferred partition.

`Fail` and `Defer` return `observation.ErrPartitionNotInBatch` for a partition
the batch does not hold. They are safe to call from goroutines the batch method
joins before it returns.

When several methods of one reactor report on the same partition, the partition
settles at the earliest event any of them reported. Everything from that event
is delivered again, including events another method already handled, so keep
side effects idempotent and use `reactors.Delivery` to recognize a redelivery.

A kernel that does not deliver pages cannot defer a partition. The client then
fails the partition with `observation.ErrDeferralNotSupportedByKernel`.

## Registration errors

Chronicle refuses to create a client whose registry breaks a batch rule. Each
error matches `chronicle.ErrInvalidConfiguration` and arrives in the reactor or
reducer `DeclarationError` naming the method:

| Error | Cause |
| --- | --- |
| `observation.ErrBatchMethodObservesNothing` | A method takes `*observation.EventBatch` but no `WithBatchMethod` names it, or `OnBatch` lists no types |
| `observation.ErrObservesOnlyOnBatchMethods` | `WithBatchMethod` names a method that does not take `*observation.EventBatch` |
| `observation.ErrBatchMethodObservesUnknownEventType` | An observed type matches no registered event |
| `observation.ErrEventTypeObservedByMoreThanOneMethod` | An event type reaches a batch method and another method |
| `observation.ErrBatchContextOnlyForBatchMethods` | A per-event method takes `observation.BatchContext` |
| `observation.ErrInvalidBatchMethodSignature` | A batch method returns events, takes the batch by value, or has other unsupported parameters |
| `observation.ErrReducerHasMoreThanOneBatchMethod` | A reducer declares two batch methods |
| `observation.ErrReducerMixesBatchAndPerEventMethods` | A reducer declares a batch method and per-event folds |

## Test a batch method

A batch method is plain Go. Build a batch with `observation.NewEventBatch`,
call the method and inspect `batch.Outcomes()`. `chronicletest.ReactorScenario`
hands the events of one `Given` call to batch methods as one live page;
`chronicletest.ReadModelScenario` folds reducers through their batch method.
