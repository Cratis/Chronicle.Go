// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type statusConn struct {
	script []any // each entry is a contracts.RegistrationState or an error
	calls  int
	last   *contracts.GetRegistrationStatusRequest
}

func (c *statusConn) Invoke(_ context.Context, _ string, args, reply any, _ ...grpc.CallOption) error {
	c.last = args.(*contracts.GetRegistrationStatusRequest)
	entry := c.script[min(c.calls, len(c.script)-1)]
	c.calls++
	if err, ok := entry.(error); ok {
		return err
	}
	reply.(*contracts.RegistrationStatus).State = entry.(contracts.RegistrationState)
	if entry == contracts.RegistrationState_Failed {
		reply.(*contracts.RegistrationStatus).Failure = "bad definition"
	}
	return nil
}

func (*statusConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unused")
}

func TestWaitForAcceptedRegistrationPollsUntilApplied(t *testing.T) {
	conn := &statusConn{script: []any{contracts.RegistrationState_Pending, status.Error(codes.Unavailable, "busy"), contracts.RegistrationState_Applied}}
	if err := waitForAcceptedRegistration(t.Context(), conn, "store", "app", time.Minute); err != nil {
		t.Fatal(err)
	}
	if conn.calls != 3 || conn.last.Registrant != "app" || conn.last.EventStore != "store" {
		t.Fatalf("calls=%d last=%v", conn.calls, conn.last)
	}
}

func TestWaitForAcceptedRegistrationFailedIsAnError(t *testing.T) {
	conn := &statusConn{script: []any{contracts.RegistrationState_Failed}}
	if err := waitForAcceptedRegistration(t.Context(), conn, "store", "app", time.Minute); !errors.Is(err, ErrRegistrationNotApplied) {
		t.Fatalf("got %v", err)
	}
}

func TestWaitForAcceptedRegistrationIgnoresUnimplementedAndEmptyRegistrant(t *testing.T) {
	conn := &statusConn{script: []any{status.Error(codes.Unimplemented, "old kernel")}}
	if err := waitForAcceptedRegistration(t.Context(), conn, "store", "app", time.Minute); err != nil || conn.calls != 1 {
		t.Fatalf("err=%v calls=%d", err, conn.calls)
	}
	if err := waitForAcceptedRegistration(t.Context(), conn, "store", "", time.Minute); err != nil || conn.calls != 1 {
		t.Fatalf("err=%v calls=%d", err, conn.calls)
	}
}

func TestWaitForAcceptedRegistrationGivesUpAfterTheBound(t *testing.T) {
	conn := &statusConn{script: []any{contracts.RegistrationState_Pending}}
	if err := waitForAcceptedRegistration(t.Context(), conn, "store", "app", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
}
