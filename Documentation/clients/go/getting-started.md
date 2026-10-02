---
title: Get started with Chronicle for Go
description: Register a typed event and append it to a local Chronicle kernel.
---

By the end, you will have a registered event type and a persisted customer event, with its sequence position printed in your terminal. Chronicle.Go handles OAuth, compatibility, registration and transport; you supply the event facts and their source identity.

## Start a development kernel

Use Go 1.26 or newer and Docker. Start the independently pinned integration kernel:

```sh
docker run --rm --name chronicle-go -p 35000:35000 cratis/chronicle:19.29.2-development
```

Wait until `curl -skf https://localhost:35000/health` returns `Healthy`. The development image includes MongoDB and a self-signed certificate. Never expose its built-in credentials on a production endpoint.

After the first Go release, install the module:

```sh
go get github.com/cratis/chronicle.go@latest
```

Before publication, run the checked-out example with `go run ./examples/getting-started` from the repository root.

## Append your first event

This complete program is compiled as [the getting-started example](../../../examples/getting-started/main.go). The source ID is event metadata, not a payload property.

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "time"

    chronicle "github.com/cratis/chronicle.go"
    "github.com/cratis/chronicle.go/events"
)

type CustomerRegistered struct {
    Name string `json:"name"`
}

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    if err := appendCustomer(ctx); err != nil {
        log.Fatal(err)
    }
}

func appendCustomer(ctx context.Context) (err error) {
    registry := chronicle.NewRegistry()
    if _, err = chronicle.RegisterEvent[CustomerRegistered](registry,
        events.WithID("customer-registered")); err != nil {
        return err
    }
    client, err := chronicle.NewClient(chronicle.WithDevelopmentDefaults(),
        chronicle.WithRegistry(registry))
    if err != nil {
        return err
    }
    defer func() { err = errors.Join(err, client.Close()) }()
    store, err := client.EventStore(ctx, "customers")
    if err != nil {
        return err
    }
    result, err := store.EventLog().Append(ctx, "customer-42", CustomerRegistered{Name: "Ada"})
    if err != nil {
        return err
    }
    if err = result.Err(); err != nil {
        return err
    }
    fmt.Printf("Appended event at position %d\n", *result.Position)
    return nil
}
```

An empty namespace starts at position `0`; later runs append at higher sequence-wide positions. Every successful run appends another fact. The program does not retry a failed write: a lost response may hide a committed event.

`EventStore` connects, ensures the default namespace and waits for event registration before publishing the handle. `Close` releases the client, including its keep-alive worker. Generated contracts target Chronicle **19.29.4**; the real-kernel smoke test exercises **19.29.2-development**, including protected first append and read-back through the public contracts.

## Continue

- [Connect securely](../../connection-strings/index.md) before leaving local development.
- [Define event types](../../events/event-types.md) for persisted names and supported JSON shapes.
- [Append with concurrency protection](../../events/appending-events.md) when a business decision depends on previously read state.
- [Review the parity map](../../parity.md) before relying on advanced client features.

Stop your foreground container with Ctrl+C. The example kernel is disposable; configure durable kernel storage separately for real data.
