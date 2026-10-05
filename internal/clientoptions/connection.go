// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package clientoptions provides module-private client construction options.
package clientoptions

import "google.golang.org/grpc"

// BorrowedTransport seals the construction seam to this module. An external
// option cannot name this internal parameter type to configure a transport.
// Connection is borrowed; its owner must close it after the client is closed.
type BorrowedTransport struct {
	Connection grpc.ClientConnInterface
}

// Connection borrows an in-memory transport for a scenario. The generic option
// preserves the root package's private configuration type without a global hook
// or an exported root option. The caller owns and closes the transport.
func Connection[Option ~func(Config), Config interface {
	BorrowConnection(BorrowedTransport)
}](conn grpc.ClientConnInterface) Option {
	return func(config Config) { config.BorrowConnection(BorrowedTransport{Connection: conn}) }
}
