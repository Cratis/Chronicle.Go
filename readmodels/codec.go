// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import "github.com/cratis/chronicle.go/internal/faults"

// CodecPanicError means an application decoder panicked. It matches ErrProtocol
// and never retains the panic value, which may contain protected content.
type CodecPanicError struct{}

// Error returns a fixed, payload-free description.
func (*CodecPanicError) Error() string { return "chronicle: read-model codec panicked" }

// Unwrap preserves the protocol category without exposing the panic payload.
func (*CodecPanicError) Unwrap() error { return faults.ErrProtocol }

// readCodec is used only at application decoder boundaries, outside transport
// leases. Ordinary errors remain inspectable; panic values are never retained.
func readCodec(decode func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = &CodecPanicError{}
		}
	}()
	return decode()
}
