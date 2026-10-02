---
title: React to events from Go
description: Register closures or convention-based reactors, return events, and optionally use shared service scopes.
---

Use a reactor to call an external service or append a follow-up event. Use a
projection to populate a read model instead. This is the experimental **part 1**
reactor API: ordinary delivery, basic returned events, middleware and scoped
activation. [The parity map](parity.md#reactors-part-1) lists the remaining work.

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
        reactors.WithID("confirm-orders"))
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

Handlers may return nothing, `error`, a registered event (or pointer), or
`(event, error)`. A nil event means no effect. A non-nil error suppresses the
returned event. Chronicle appends a returned event to **the event log**, even if
the reactor observes another sequence, using the triggering source ID unless the
reactor implements `EventSourceIDProvider`. Append rejection fails handling.
Collections, targeted wrappers, custom effects and route/subject providers are
not implemented in part 1.

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
A registered artifact takes precedence over its constructor; a nil constructor
requests service activation. When the factory supplies Fundamentals' optional
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

`reactors.WithMiddleware(factory)` activates a `reactors.Middleware` from the
same scope as the reactor. Its `Before` and `After` hooks receive an `Invocation`
with the event, context, delivery identity and borrowed scope. Hooks run in
registration order. All before hooks run; any before error prevents handling.
After hooks run even when handling fails; their errors are logged and do not
change the acknowledgement after effects have run.

The optional `services.Scope(invocation.Scope)` unwraps the Fundamentals scope.
Arc adapters can check `ScopeOwner`/`ContextChecker` before borrowing it. They
must not close it, retain it or assume that a delivery joins a command's
transaction. Delivery IDs support external idempotency records, not atomicity
between an external effect and a Chronicle append.

## Delivery, failure and shutdown

Event types and read models register before constraints/projections; reactor
streams start after that registration pass succeeds. The duplex protocol has no
registration acknowledgement: a successful store/readiness call means reactor
registration was **sent**, not that a handler has caught up.

Events execute in received order, with no application queue. A failed event stops
the batch; the result names only the last successful sequence number, or
`events.Unavailable` when none succeeded. The kernel owns partition recovery and
quarantine. The client never retries an effect by itself.

Cleanup completes before acknowledgement. Unlike C#'s swallowed disposal errors,
a cleanup failure fails the batch; with a batch scope, its successful position
resets to unavailable. **Effects may already have happened and can repeat on
kernel recovery.** Make them idempotent. Middleware activation also fails closed,
rather than C#'s log-and-skip policy.

Stream failure replaces the existing connection generation, joins old workers,
and re-registers frozen plans. The kernel retains progress; the client does not
invent a resume position. This can interrupt other observers on that generation.
`store.UnregisterReactor(ctx, id)` cancels and joins a local reactor, retaining its
removal across reconnect. It does not delete kernel state. Do not synchronously
unregister a reactor from its own handler.

`Close` cancels and joins workers. `CloseContext` bounds the caller's wait when
user code ignores cancellation; a timeout means cleanup is still incomplete.
Replay replacement/OnceOnly, read-model reactors, observer administration and
completion/tail APIs remain outside this slice.
