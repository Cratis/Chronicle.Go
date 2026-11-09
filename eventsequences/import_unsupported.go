// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//go:build !chronicle_imports

package eventsequences

import (
	"context"
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
)

// The pinned contracts (see contracts.ProtocolVersion) predate the kernel's
// Imports service, so every import operation fails with ErrUnsupported until the
// pin moves to a Chronicle release that carries it. Building with the
// chronicle_imports tag and locally generated contracts/imports binds the real
// service; that build is for verification only.
func newImportTransport(*Sequence) importTransport { return unsupportedImports{} }

var errImportsUnsupported = fmt.Errorf("%w: the kernel protocol this client is built against has no Imports service", faults.ErrUnsupported)

type unsupportedImports struct{}

func (unsupportedImports) begin(context.Context, ImportID, OnInvalidEvent) (importOpened, error) {
	return importOpened{}, errImportsUnsupported
}

func (unsupportedImports) addChunk(context.Context, importChunk) (importChunkAck, error) {
	return importChunkAck{}, errImportsUnsupported
}

func (unsupportedImports) complete(context.Context, ImportID, int32, int64) (importStatus, error) {
	return importStatus{}, errImportsUnsupported
}

func (unsupportedImports) abandon(context.Context, ImportID) (importStatus, error) {
	return importStatus{}, errImportsUnsupported
}

func (unsupportedImports) get(context.Context, ImportID) (importSnapshot, bool, error) {
	return importSnapshot{}, false, errImportsUnsupported
}

func (unsupportedImports) observe(context.Context, ImportID, func(importSnapshot) bool) error {
	return errImportsUnsupported
}
