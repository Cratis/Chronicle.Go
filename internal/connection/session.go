// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"reflect"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const clientIsNotConnected = "ClientIsNotConnected"

// grpcStatusError is grpc-go's own status error type. Only that exact type is
// inspected: an observer stream can end with an application error, and the SDK
// never calls Error, Is, As, Unwrap or GRPCStatus on application errors.
var grpcStatusError = reflect.TypeOf(status.Error(codes.Unknown, ""))

// SessionLost reports whether err is the kernel saying it no longer knows the
// connection ID a stream was opened under: it restarted or evicted this client.
// Retrying under that ID can never work; only a new connection can recover.
func SessionLost(err error) bool {
	if err == nil || reflect.TypeOf(err) != grpcStatusError {
		return false
	}
	return strings.Contains(status.Convert(err).Message(), clientIsNotConnected)
}
