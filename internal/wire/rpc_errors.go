// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package wire

import (
	"context"
	"errors"

	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RPCError preserves transport status and adds stable SDK/context identities.
func RPCError(err error) error {
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.Unimplemented:
		return errors.Join(faults.ErrUnsupported, err)
	case codes.Canceled:
		return errors.Join(context.Canceled, err)
	case codes.DeadlineExceeded:
		return errors.Join(context.DeadlineExceeded, err)
	default:
		return err
	}
}
