// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSessionLostEndsTheGenerationAsErrSessionLost(t *testing.T) {
	g := &generation{}
	g.bind(t.Context())
	kernel := status.Error(codes.Internal, "ClientIsNotConnected")
	g.loseSession(kernel)
	if g.ctx.Err() == nil {
		t.Fatal("a lost session left the generation running")
	}
	err := generationEnded(g, context.Canceled)
	if !errors.Is(err, ErrSessionLost) || !errors.Is(err, kernel) {
		t.Fatalf("generation ended with %v, want ErrSessionLost wrapping the kernel status", err)
	}
}

func TestSessionLostIsNeverTerminalWhateverTheKernelCode(t *testing.T) {
	err := fmt.Errorf("%w: %w", ErrSessionLost, status.Error(codes.InvalidArgument, "ClientIsNotConnected"))
	if terminalConnectionError(err) {
		t.Fatal("a lost session stopped the client from reconnecting")
	}
}

func TestLateSessionLostReportFromAnEndedGenerationChangesNothing(t *testing.T) {
	old, current := &generation{}, &generation{}
	old.bind(t.Context())
	current.bind(t.Context())
	stale := errors.New("keep-alive ended")
	old.cancel()
	old.loseSession(status.Error(codes.Internal, "ClientIsNotConnected"))
	if err := generationEnded(old, stale); err != stale {
		t.Fatalf("ended generation reported %v, want its original end %v", err, stale)
	}
	if current.ctx.Err() != nil {
		t.Fatal("a report from an ended generation ended the current one")
	}
}
