// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"

	"github.com/cratis/chronicle.go/internal/connection"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func terminalConnectionError(err error) bool {
	var selection *LoadBalancerError
	if errors.As(err, &selection) {
		return false
	}
	var incompatible *CompatibilityError
	var auth *connection.AuthenticationError
	if errors.As(err, &incompatible) || errors.As(err, &auth) || errors.Is(err, ErrProtocol) || errors.Is(err, ErrClosed) {
		return true
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied, codes.Unimplemented, codes.InvalidArgument:
		return true
	default:
		return false
	}
}
