// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package connection

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type inspectedError struct{ inspected bool }

func (e *inspectedError) Error() string { e.inspected = true; return "ClientIsNotConnected" }
func (e *inspectedError) GRPCStatus() *status.Status {
	e.inspected = true
	return status.New(codes.Internal, "ClientIsNotConnected")
}

func TestSessionLost(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"kernel does not know the connection", status.Error(codes.Internal, "Observer failed: ClientIsNotConnected"), true},
		{"unknown status code still names it", status.Error(codes.Unknown, "ClientIsNotConnected"), true},
		{"other kernel failure", status.Error(codes.Unavailable, "kernel stopping"), false},
		{"unavailable naming it is a transport condition", status.Error(codes.Unavailable, "ClientIsNotConnected"), false},
		{"cancelled naming it is a transport condition", status.Error(codes.Canceled, "ClientIsNotConnected"), false},
		{"end of stream", io.EOF, false},
		{"nil", nil, false},
		{"plain error naming it", errors.New("ClientIsNotConnected"), false},
		{"wrapped status is not trusted", fmt.Errorf("wrapped: %w", status.Error(codes.Internal, "ClientIsNotConnected")), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SessionLost(tc.err); got != tc.want {
				t.Fatalf("SessionLost = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestSessionLostNeverInspectsApplicationErrors(t *testing.T) {
	err := &inspectedError{}
	if SessionLost(err) || err.inspected {
		t.Fatal("an application error was inspected as a kernel status")
	}
}
