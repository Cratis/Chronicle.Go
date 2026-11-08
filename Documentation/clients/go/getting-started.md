---
title: Get started with Chronicle for Go
description: Install the Go client, register a typed event and append it to a local Chronicle kernel, then confirm the append outcome in your own first program.
---

The maintained Go example below registers a typed customer event, appends it and prints its sequence position on success. A live run needs the development kernel started below. Begin with the [capability and limits summary](../../parity.md#v0-capabilities-and-limits), especially before using observers or decision reads. Chronicle.Go handles OAuth, compatibility, registration and transport; you supply the event facts and their source identity.

## Start a development kernel

Use Go 1.26 or newer and Docker. Start the integration kernel matching the contract release:

```sh
docker run --rm --name chronicle-go -p 127.0.0.1:35000:35000 cratis/chronicle:19.125.2-development
```

Wait until `curl -skf https://localhost:35000/health` returns `Healthy`; that establishes process health, not feature compatibility. The development image includes MongoDB and a self-signed certificate. Never expose its built-in credentials on a production endpoint.

## Select a release or source checkout

Go v0.x releases exist; see [release policy](../../releases.md) before selecting a version. The command below asks Go to resolve the latest module. A GitHub release does not by itself establish its module archive contents or kernel compatibility:

```sh
go get github.com/cratis/chronicle.go@latest
```

For the repository source example, use `go run ./examples/getting-started` from the repository root after providing a compatible endpoint. Do not assume a tagged module contains every API on current main.

## Append your first event

This program is maintained as [the getting-started example](https://github.com/Cratis/Chronicle.Go/blob/main/examples/getting-started/main.go). It uses a bounded operation context, a normal client and explicit cleanup; it is not a runnable `WithSkipKeepAlive` function/job example. The source ID is event metadata, not a payload property.

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

`EventStore` connects, ensures the default namespace and waits for event registration before publishing the handle. Registration sent is not observer subscription or catch-up. `Close` releases the client, including its keep-alive worker. Read [short-lived clients and process ownership](../../connection-strings/lifecycle.md#short-lived-clients-and-process-ownership) for skipped-session exclusions and process reuse.

Generated contracts target Chronicle **19.125.2**, and the real-kernel tests run against **19.125.2-development**. [Decision reads](../../read-models/decision-reads.md) have their own exact server/protocol and projection-admission requirements.

## Continue

- [Connect securely](../../connection-strings/index.md) before leaving local development.
- [Define event types](../../events/event-types.md) for persisted names and supported JSON shapes.
- [Append with concurrency protection](../../events/appending-events.md) when a business decision depends on previously read state.
- [Review the parity map](../../parity.md) before relying on advanced client features.

Stop your foreground container with Ctrl+C. The example kernel is disposable; configure durable kernel storage separately for real data.
