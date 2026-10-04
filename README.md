# Chronicle for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/cratis/chronicle.go.svg)](https://pkg.go.dev/github.com/cratis/chronicle.go)
[![Build](https://github.com/Cratis/Chronicle.Go/actions/workflows/build.yml/badge.svg)](https://github.com/Cratis/Chronicle.Go/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/Cratis/Chronicle.Go/blob/main/LICENSE)

The idiomatic Go client for [Cratis Chronicle](https://github.com/Cratis/Chronicle). Register typed events, connect securely, select a store and namespace, and append with explicit concurrency protection and complete outcomes.

## Status and installation

**Experimental foundation, v0.x.** Connection/TLS/OAuth, compatibility, event registration, single/atomic batch appends and event history reads are implemented. Observers, projections and automatic lifecycle recovery are not yet implemented. See [parity and limitations](https://github.com/Cratis/Chronicle.Go/blob/main/Documentation/parity.md).

Requires Go **1.26 or later**. After the first tagged release:

```sh
go get github.com/cratis/chronicle.go@latest
```

Contracts are pinned to Chronicle **19.29.4**; real-kernel tests use **19.29.4-development**. Generated contracts are public packages in this same module, with no .NET or sibling checkout needed.

## Quick start

Start the development kernel and wait for `https://localhost:35000/health` to report `Healthy`:

```sh
docker run --rm --name chronicle-go -p 35000:35000 cratis/chronicle:19.29.4-development
```

This complete program is also available as `go run ./examples/getting-started` in a checkout:

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

`WithDevelopmentDefaults` explicitly permits the kernel's self-signed certificate. Production TLS validates by default: configure real roots and credentials. An append transport error may hide a committed event; **never blindly retry**. Default optimistic concurrency leaves empty history unchecked; use `NoMatchingEvent` when first append must be protected.

## Documentation and development

- [Getting started](https://github.com/Cratis/Chronicle.Go/blob/main/Documentation/clients/go/getting-started.md)
- [Connecting and lifecycle](https://github.com/Cratis/Chronicle.Go/blob/main/Documentation/connection-strings/index.md)
- [Event types](https://github.com/Cratis/Chronicle.Go/blob/main/Documentation/events/event-types.md)
- [Declared Int32 enum codecs](https://github.com/Cratis/Chronicle.Go/blob/main/Documentation/enum-codecs.md)
- [Appending and concurrency](https://github.com/Cratis/Chronicle.Go/blob/main/Documentation/events/appending-events.md)
- [Atomic batches](https://github.com/Cratis/Chronicle.Go/blob/main/Documentation/events/batches.md)
- [Reading events and history](https://github.com/Cratis/Chronicle.Go/blob/main/Documentation/events/reading-events.md)
- [Contributing and required checks](https://github.com/Cratis/Chronicle.Go/blob/main/CONTRIBUTING.md)

```sh
export GOWORK=off
go build ./...
go vet ./...
go test -race -count=1 -timeout=3m ./...
golangci-lint run
go generate ./...
python3 scripts/generate-contracts.py --check
```

## Community, security and license

[Cratis](https://www.cratis.io/) · [Cratis repositories](https://github.com/Cratis) · [Private vulnerability reporting](https://github.com/Cratis/Chronicle.Go/blob/main/SECURITY.md) · [MIT license](https://github.com/Cratis/Chronicle.Go/blob/main/LICENSE)
