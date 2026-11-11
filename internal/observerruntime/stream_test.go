// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type message struct{ id int }

// scriptedReceive returns each step in order: a message, or an error.
func scriptedReceive(steps ...any) func() (*message, error) {
	return func() (*message, error) {
		step := steps[0]
		steps = steps[1:]
		if err, ok := step.(error); ok {
			return nil, err
		}
		return step.(*message), nil
	}
}

func notConnected() error {
	return status.Error(codes.Internal, "Observer subscription failed: ClientIsNotConnected")
}

func TestStreamReceivingAnUnknownConnectionIsASessionLoss(t *testing.T) {
	err := runStream(t.Context(), scriptedReceive(notConnected()), nil, func(context.Context, *message) (*message, error) { return nil, nil }, func(*message) error { return nil })
	if !SessionLost(err) {
		t.Fatalf("SessionLost(%v) = false, want true", err)
	}
}

func TestHandlerErrorNamingAnUnknownConnectionIsNotASessionLoss(t *testing.T) {
	application := notConnected()
	err := runStream(t.Context(), scriptedReceive(&message{}), nil, func(context.Context, *message) (*message, error) { return nil, application }, func(*message) error { return nil })
	if err != application {
		t.Fatalf("runStream returned %v, want the handler's own error", err)
	}
	if SessionLost(err) {
		t.Fatal("a status error returned by a handler ended the client's connection")
	}
}

func TestSendEndedByTheKernelReportsTheStatusItEndedWith(t *testing.T) {
	delivered := 0
	err := runStream(t.Context(), scriptedReceive(&message{id: 1}, &message{id: 2}, notConnected()), func() { delivered++ }, func(context.Context, *message) (*message, error) { return &message{}, nil }, func(*message) error { return io.EOF })
	if !SessionLost(err) {
		t.Fatalf("SessionLost(%v) = false, want true", err)
	}
	if delivered != 1 {
		t.Fatalf("delivered = %d, want 1: a message drained from an ended stream was never handled", delivered)
	}
}

func TestSendEndedWithoutAStatusReportsEndOfStream(t *testing.T) {
	err := sendOnStream(func(*message) error { return io.EOF }, scriptedReceive(io.EOF), &message{})
	if !errors.Is(err, io.EOF) || SessionLost(err) {
		t.Fatalf("sendOnStream returned %v, want end of stream", err)
	}
}

func TestSendFailingOtherwiseDoesNotReceive(t *testing.T) {
	failure := status.Error(codes.Unavailable, "gone")
	err := sendOnStream(func(*message) error { return failure }, func() (*message, error) {
		t.Fatal("received after a send failure that was not end of stream")
		return nil, nil
	}, &message{})
	if !errors.Is(err, failure) {
		t.Fatalf("sendOnStream returned %v, want %v", err, failure)
	}
}
