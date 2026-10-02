# Chronicle for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/cratis/chronicle.go.svg)](https://pkg.go.dev/github.com/cratis/chronicle.go)
[![Build](https://github.com/Cratis/Chronicle.Go/actions/workflows/build.yml/badge.svg)](https://github.com/Cratis/Chronicle.Go/actions/workflows/build.yml)
[![Release](https://github.com/Cratis/Chronicle.Go/actions/workflows/publish.yml/badge.svg)](https://github.com/Cratis/Chronicle.Go/actions/workflows/publish.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

The Go client for [Cratis Chronicle](https://github.com/Cratis/Chronicle), the event-sourcing platform in the Cratis ecosystem.

## Status

**Early development.** This repository currently contains the module and repository scaffold, not implemented client APIs. Releases will remain **v0.x** while the API is experimental. There is no tagged Go release yet; the installation command and Go reference will become usable after the first release.

Usage examples will follow as APIs are implemented. No kernel or protocol compatibility is claimed yet.

## Installation

Requires Go **1.26 or later**. Once a version has been published:

```sh
go get github.com/cratis/chronicle.go@latest
```

Use the lowercase module path exactly as shown. CI checks Go 1.26 and 1.27 independently of local Go workspaces.

## Documentation

Start with [Documentation](Documentation/index.md). API reference will be available on [pkg.go.dev](https://pkg.go.dev/github.com/cratis/chronicle.go) after publication.

## Development

From the repository root:

```sh
export GOWORK=off
go build ./...
go vet ./...
go test -race -count=1 -timeout=3m ./...
golangci-lint run
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full checks and release conventions.

## Community and security

- [Cratis](https://www.cratis.io/) and the [Cratis repositories](https://github.com/Cratis)
- [Contribution guide](CONTRIBUTING.md)
- [Private vulnerability reporting](SECURITY.md)

## License

[MIT](LICENSE).
