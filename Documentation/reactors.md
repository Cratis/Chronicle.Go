---
title: React to events from Go
description: Register closures or convention-based reactors, return events, and optionally use shared service scopes.
---

Use a reactor to call an external service or append a follow-up event. Use a
projection to populate a read model instead. This experimental API supports
convention discovery, replay policies, returned effects and scoped activation.
[The parity map](parity.md#reactors-part-2) identifies the remaining gaps.

## Plain Go first

The [complete program](../examples/reactors/main.go) registers `OrderPlaced`,
registers a typed closure, connects, appends an order and waits for its reactor.
It imports neither a container nor the dependency-injection package.

With a development kernel listening on localhost:35000, run:

```sh
go run ./examples/reactors
```

The program prints `Handled order for Chronicle`. It uses development TLS and
credentials, creates a new namespace under `plain-go-reactors` on each run, and
leaves its persisted events there. Do not use its development connection settings
in production.

The registration entry point is:

```go
err := chronicle.RegisterReactorHandler(registry, "orders",
    func(ctx context.Context, event OrderPlaced) error {
        return notify(ctx, event.Product)
    })
```

This excerpt assumes the example's registered `OrderPlaced` and your own
`notify(context.Context, string) error` function. Check `err` before constructing
the client. Captured collaborators remain yours to close. Additional callbacks
use `reactors.WithHandler(reactors.On(...))`; a typed returned-event callback uses
`reactors.WithHandler(reactors.Returning(...))`. To register only returned-event
callbacks, use `chronicle.RegisterReactorHandlers(registry, id, []reactors.Handler{...})`.

## Convention-based reactors

Register a type explicitly, then let Chronicle discover its exported methods.
Method names are descriptive, not reserved: an optional leading `context.Context`
is followed by a registered event, a pointer to one, or a nonempty interface
implemented by registered events. An unconstrained `any` is not a handler.

The following excerpt comes from the [compiling convention example](../examples/reactors/convention.go).
`OrderPlaced` is the event above; `ReservationGateway` is an application-owned
interface with `Reserve(context.Context, string, string) error`, where the last
argument is an idempotency key. Call `registerConfirmOrders` with an empty registry
and your gateway, check its error, then pass `chronicle.WithRegistry(registry)` to
`NewClient`.

```go
// OrderConfirmed is the durable outcome of reserving an order's product.
type OrderConfirmed struct {
    Product string `json:"product"`
}

type ConfirmOrders struct {
    reservations ReservationGateway
}

// Reserve is discovered by OrderPlaced, not by its method name.
func (r *ConfirmOrders) Reserve(ctx context.Context, event OrderPlaced, delivery reactors.Delivery) (OrderConfirmed, error) {
    if err := r.reservations.Reserve(ctx, event.Product, delivery.ID()); err != nil {
        return OrderConfirmed{}, err
    }
    return OrderConfirmed(event), nil
}

func registerConfirmOrders(registry *chronicle.Registry, gateway ReservationGateway) error {
    if _, err := chronicle.RegisterEvent[OrderPlaced](registry); err != nil {
        return err
    }
    if _, err := chronicle.RegisterEvent[OrderConfirmed](registry); err != nil {
        return err
    }
    return chronicle.RegisterReactor[*ConfirmOrders](registry,
        func() *ConfirmOrders { return &ConfirmOrders{reservations: gateway} },
        reactors.WithID("confirm-orders"), reactors.OnceOnly("Reserve"))
}
```

`NewClient` compiles and validates every candidate against that registry. It does
not call constructors. Competing methods use descending parameter count, then
ordinal method name; shadowed methods produce diagnostics. Prefer one method per
event. Duplicate explicit callbacks fail rather than silently overriding a method.
Go cannot reflectively invoke unexported methods; use an exported method or an
explicit callback. Unknown types are not inferred as events; an explicit callback
for an unregistered type and a reactor with no handlers both fail construction.

The default observer ID is the full Go import path plus the type name, without a
pointer marker. Use `WithID` to keep a persisted identity across renames or to
share an existing C# identity. `WithEventSequence` selects a sequence; otherwise
reactors observe the event log.

### Handler parameters and results

After the event, parameters resolve in this order:

1. `events.Context`: persisted event metadata, with the selected generation.
2. `reactors.Delivery`: reactor, store, namespace, sequence, source partition and
   sequence number. `ID()` combines these with `#`, like C#; it is not an
   exactly-once guarantee. Components containing `#` retain C#'s ambiguity.
3. A registered read model: fetched by the event source ID. Use
   `WithReadModelKey` or implement `ReadModelKeyResolver` for another key. A missing
   model is a nil pointer; requesting a value when it is absent fails handling.
   Materialized reads remain eventually consistent.
4. A service from the batch scope.

Handlers may return nothing, `error`, a supported effect, or `(effect, error)`.
A nil result means no effect; a non-nil error suppresses all returned effects.
Unsupported declared results fail at `NewClient`, before connection work.

### Returning events and batches

Return events rather than injecting an event log. These results append to **the
event log**, even when the reactor observes another sequence:

| Result | Operation |
| --- | --- |
| Registered event or pointer | One append |
| Slice/array of registered events | One atomic `AppendMany`, in order |
| `eventsequences.Entry` or pointer | Self-describing source, route, subject, tags, occurrence and causation |
| Slice/array of entries | Ordered atomic `AppendBatch` |
| `[]any` mixing bare events and entries | One atomic batch, preserving each entry's metadata |
| `eventsequences.EventsWithConcurrencyScopes` | Ordered `Events` and explicit `Scopes` via `AppendBatch` |

Bare events default to the triggering source ID. `EventSourceIDProvider`,
`EventStreamIDProvider` and `SubjectProvider` on the activated reactor override
source, stream ID and subject. `WithEventSourceType`, `WithEventStreamType` and
`WithEventStreamID` supply static defaults; the stream ID provider wins over the
option. Without a subject provider, normal event subject resolution applies.
**Entries are self-describing:** reactor defaults never overwrite them.

Empty collections are no-ops; nil elements and unregistered values fail handling.
An empty concurrency batch still needs a protected scope. Classification precedes
writes and the append APIs validate/serialize the complete event batch before I/O.
Constraint, concurrency, authorization and transport failures prevent acknowledgement.
No client effect retry is added. See the [compiled effects example](../examples/reactors/effects.go).

### Replay without repeating external effects

Use `reactors.OnceOnly()` to mark an entire observer not replayable. Use
`reactors.OnceOnly("Reserve")` to skip only that exported handler during replay.
For typed callbacks, use `reactors.Returning(...).OnceOnly()` or
`reactors.On(...).OnceOnly()`.

`reactors.Replay("Rebuild")` selects an exported replacement handler. During replay
it runs **instead of** the ordinary handler for that event. Without a replacement,
the ordinary handler runs unless marked OnceOnly. Replay-only handlers still
contribute subscriptions. Typed callbacks use `.DuringReplay()`. All method names
are validated at construction; `Replay...` and `Once...` name prefixes have no
special meaning. If a replacement itself is OnceOnly, replay skips it without
falling back to the live handler.

OnceOnly is replay exclusion, **not deduplication**: recovering a failed partition
can invoke the same ordinary handler again. Keep external effects idempotent.

Implement `reactors.ReplayNotifier` for `BeginReplay`/`EndReplay` and
`PartitionReplayNotifier` for partition notifications. Each notification gets a
separate scope and artifact, never an event middleware chain or a reused batch
instance. Activation failures are logged like C#; notification/cleanup errors
terminate the stream and trigger its normal resubscription. Notifications have no
acknowledgement message in the protocol.

### Filters are not labels

`WithEventTagFilter("a", "b")` admits events having either tag. Tags combine with
source/stream filters using AND. `WithTags(...)` only labels the reactor; labels
never filter. `WithEventStreamID` is output metadata, not an input filter.

`WithEventSequence` and `WithEventLog` explicitly select the input sequence and
suppress automatic subscriptions. Without them, `events.WithSourceStore` metadata
selects the local log or an external inbox; registration provisions the shared
subscription. See [external integrations](integrations/index.md) for precedence,
observer-level source overrides and outbox publication.

## Dependency injection is opt-in

The core has a small `reactors.ScopeFactory` boundary, not another container.
To use a Fundamentals.Go factory, import the optional adapter:

```go
import "github.com/cratis/chronicle.go/services"

// provider is a caller-owned dependencyinjection.ScopeFactory.
client, err := chronicle.NewClient(
    chronicle.WithRegistry(registry),
    services.WithServices(provider),
)
```

Check `err` and close the client before its provider. Constructors can take an
optional leading context and service parameters, returning `R` or `(R, error)`.
An artifact advertised by the provider's `Catalog` takes precedence over its
constructor; a nil constructor requests service activation even without a catalog.
Without a catalog, an explicit constructor remains the activation path rather
than guessing whether a failed resolution means an absent service. When the factory supplies Fundamentals' optional
`Catalog`, `NewClient` checks constructor and handler service parameters against
it. Without `Catalog`, service availability is checked at invocation, as agreed
for C#'s permissive default-provider counterpart.

One received batch gets **one scope, reactor and middleware chain** by default.
`reactors.PerEvent()` explicitly selects fresh instances for every event instead.
`reactors.BatchFromContext` exposes store/namespace/sequence/reactor coordinates
before scope activation; a batch constructor must not capture a first event's
identity. Handler contexts carry per-event correlation, system/on-behalf-of
identity and reactor causation. There is no resolver hidden in context.

Scope-resolved instances belong to their provider. Constructor results belong to
Chronicle and are closed in reverse order, preferring `Close(context.Context)`
over `Close()`. Plain closure captures remain caller-owned. Client-lifetime
collaborators should have **Singleton** bindings; do not retain an operation scope
for the client's lifetime.

### Middleware and Arc adapters

`chronicle.RegisterReactorMiddleware(registry, factory)` applies a middleware to
every reactor in that registry. `reactors.WithMiddleware(factory)` adds one to a
single reactor. Both activate a `reactors.Middleware` from the same scope as the
reactor. Its `Before` and `After` hooks receive an `Invocation` with the event,
context, delivery identity and borrowed scope. Registry middleware runs first,
then per-reactor middleware, each in registration order. `NewClient` freezes the
registry list; later additions do not change existing clients. All before hooks run; any before error prevents handling.
After hooks run even when handling fails; their errors are logged and do not
change the acknowledgement after effects have run.

The optional `services.Scope(invocation.Scope)` unwraps the Fundamentals scope.
Arc adapters can check `ScopeOwner`/`ContextChecker` before borrowing it. They
must not close it, retain it or assume that a delivery joins a command's
transaction. Delivery IDs support external idempotency records, not atomicity
between an external effect and a Chronicle append.

### Returned commands and custom effects

Register a `reactors.SideEffectHandler` with
`chronicle.RegisterReactorSideEffectHandler(registry, handler)` **before**
`NewClient`, or use `reactors.WithSideEffectHandlers(...)` on one reactor.
The [compiled command adapter](../examples/reactors/effects.go) demonstrates the
extension without an Arc dependency. Arc can resolve its pipeline from the
borrowed batch scope and return validation/authorization failures as errors.
Do not execute returned commands in `After`: those errors are intentionally logged
without failing handling.

`CanHandleReturnType` participates in startup validation. `CanHandle` must only
classify: every matching handler is selected before any append or execution.
`Handle` receives `SideEffectContext`, including delivery identity, event context,
reactor instance, store-local catalog, borrowed scope and `Replay`, `OnceOnly`,
`Replayable` policy. Handlers are borrowed, concurrency-safe instances; Chronicle
does not dispose them. Registrations freeze separately for each store catalog.

All matching extensions run in registration order, even if an earlier extension
returns an error; errors are joined and fail handling before acknowledgement.
The built-in event handler also runs when it matches. A handler can claim an entire
collection. Otherwise a collection of custom/mixed items is fully classified
before any item runs, then executed in order, stopping after a failed item. A
`[]Command` return is admitted when the command element type is claimed. Unknown
elements of `[]any` fail before any effects. Lazy or recursively nested collections
need an extension claiming that shape. Pure event collections remain atomic;
command/event mixtures and external operations **are not one transaction**.
Already completed effects can repeat on recovery.

## Delivery, failure and shutdown

Event types and read models register before constraints/projections; reactor
streams start after that registration pass succeeds. The duplex protocol has no
registration acknowledgement: a successful store/readiness call means reactor
registration was **sent**, not that a handler has caught up. A caller's deadline
bounds this readiness wait, not the subscription lifetime. Failed stream opens
retry independently; unregistering during an open releases readiness waiters.

### Wait for observers before the first append

`EventStore` and `WaitForRegistration` mean registration was sent, not that the
kernel has subscribed your observers. On the pinned 19.29.4 kernel, a reactor or
reducer that subscribes after events were appended to a key makes the kernel start
a catch-up for that partition. Events appended to the same key during that catch-up
can be dropped for other observers, including projections. This is an upstream
defect ([Chronicle#4558](https://github.com/Cratis/Chronicle/issues/4558)); the
client cannot repair it.

As a startup mitigation, wait until your observers are subscribed and `Active`
before the first append whose live delivery matters, using a bounded context:

```go
func awaitObserversActive(ctx context.Context, store *chronicle.EventStore, ids ...observation.ID) error {
    ticker := time.NewTicker(50 * time.Millisecond)
    defer ticker.Stop()
    for _, id := range ids {
        for {
            info, err := store.Observers().Get(ctx, id, events.EventLog)
            if err != nil {
                return err
            }
            if info != nil && info.IsSubscribed() && info.RunningState() == observation.Active {
                break
            }
            select {
            case <-ctx.Done():
                return fmt.Errorf("observer %s is not subscribed and active: %w", id, ctx.Err())
            case <-ticker.C:
            }
        }
    }
    return nil
}
```

Pass your reactor and reducer IDs. If projections must see the first events, also
wait for the kernel's event-log observers `$system.statistics.event-types` and
`$system.statistics.event-types.global`, as the integration tests do. This does
not protect observers added later or after a reconnect.

Events execute in received order, with no application queue. A failed event stops
the batch; the result names only the last successful sequence number, or
`events.Unavailable` when none succeeded. The kernel owns partition recovery and
quarantine. The client never retries an effect by itself.

Cleanup completes before acknowledgement. Unlike C#'s swallowed disposal errors,
a cleanup failure fails the batch; with a batch scope, its successful position
resets to unavailable. **Effects may already have happened and can repeat on
kernel recovery.** Make them idempotent. Middleware activation also fails closed,
rather than C#'s log-and-skip policy.

When a reactor stream fails or completes, only that reactor resubscribes after
two seconds on the same connection. This does not cancel unrelated appends or
observers. Unregister and shutdown cancel the retry delay. The keep-alive
watchdog owns recovery from actual connection loss: a new generation re-registers
frozen plans without waiting for old handlers to finish. The kernel retains
progress; the client does not invent a resume position.
`store.UnregisterReactor(ctx, id)` cancels and joins a local reactor, retaining its
removal across reconnect. It does not delete kernel state. Do not synchronously
unregister a reactor from its own handler.

`Close` cancels and joins workers, including observers from retired generations.
Old owned channels close only after their observers finish. A handler that
ignores cancellation cannot block reconnection or unrelated RPCs, but it can
outlive its generation and overlap delivery on the new one. Honor cancellation
and make effects idempotent. `CloseContext` bounds the caller's wait when user
code ignores cancellation; a timeout means cleanup is still incomplete.
Read-model reactors (`Added`/`Modified`/`Removed` watches) remain with
[#33](https://github.com/Cratis/Chronicle.Go/issues/33). Observer administration,
completion/tail waits and local append-result notifications remain in the
observer follow-up (#13 of the plan). Historical payload selection works for the
one registered descriptor per ID; simultaneous historical codecs remain with
[#32](https://github.com/Cratis/Chronicle.Go/issues/32).
