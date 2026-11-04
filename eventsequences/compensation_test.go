// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type reopened struct {
	Value string `json:"value"`
}

func compensationFixture(t *testing.T, handler rpcHandler) (*eventsequences.Sequence, func() int) {
	t.Helper()
	original, err := events.Define[opened](events.WithID("opened"))
	if err != nil {
		t.Fatal(err)
	}
	compensation, err := events.Define[reopened](events.WithID("reopened"), events.WithTags("static", "shared"), events.WithCompensationFor(original))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := events.Define[changed](events.WithID("changed"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(original.Descriptor(), compensation.Descriptor(), plain.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	sequence, calls := parityFixture(t, map[string]rpcHandler{"Compensate": handler}, catalog, eventsequences.ConcurrencyPolicy{})
	return sequence, func() int { return int(calls.Load()) }
}

func TestCompensateSendsTheCompensationRequest(t *testing.T) {
	occurred := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var got *sequences.CompensateRequest
	sequence, calls := compensationFixture(t, func(_ context.Context, request any) (any, error) {
		got = request.(*sequences.CompensateRequest)
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, SequenceNumber: 9}}, nil
	})
	correlation, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	result, err := sequence.Compensate(testContext(t), 0, reopened{"again"}, eventsequences.WithCorrelation(correlation), eventsequences.WithTags("shared", "dynamic"), eventsequences.WithOccurred(occurred))
	if err != nil || result.Disposition != eventsequences.Committed || result.Position == nil || *result.Position != 9 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if calls() != 1 || got.EventStore != "store" || got.Namespace != "tenant" || got.EventSequenceId != "event-log" {
		t.Fatalf("calls=%d request=%v", calls(), got)
	}
	if got.CompensationFor != 0 || got.EventType.Id != "reopened" || got.EventType.Generation != 1 || got.Content != `{"value":"again"}` {
		t.Fatalf("request=%v", got)
	}
	if want := []string{"static", "shared", "dynamic"}; len(got.Tags) != 3 || got.Tags[0] != want[0] || got.Tags[1] != want[1] || got.Tags[2] != want[2] {
		t.Fatalf("tags=%v", got.Tags)
	}
	if got.Occurred.Value != "2026-01-02T03:04:05.0000000+00:00" {
		t.Fatalf("occurred=%v", got.Occurred)
	}
}

func TestCompensateKnownRejectionsAreRejectedResults(t *testing.T) {
	for _, code := range []eventsequences.AppendError{eventsequences.CompensatedEventNotFound, eventsequences.EventTypeDoesNotCompensateEventType} {
		sequence, _ := compensationFixture(t, func(context.Context, any) (any, error) {
			return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{HasErrors: true, Errors: []string{string(code)}}}, nil
		})
		result, err := sequence.Compensate(testContext(t), 4, reopened{"x"})
		var unknown *eventsequences.OutcomeUnknownError
		if err != nil || result.Disposition != eventsequences.Rejected || len(result.Errors) != 1 || result.Errors[0] != code {
			t.Fatalf("%s: result=%+v err=%v", code, result, err)
		}
		if err := result.Err(); err == nil || errors.As(err, &unknown) || !errors.Is(err, code) {
			t.Fatalf("%s: promoted error %v", code, err)
		}
	}
}

func TestCompensateUnrecognizedErrorsStayUnknown(t *testing.T) {
	sequence, _ := compensationFixture(t, func(context.Context, any) (any, error) {
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{HasErrors: true, Errors: []string{"SomethingNew"}}}, nil
	})
	_, err := sequence.Compensate(testContext(t), 4, reopened{"x"})
	var unknown *eventsequences.OutcomeUnknownError
	if !errors.As(err, &unknown) {
		t.Fatalf("err=%v", err)
	}
}

func TestCompensateRefusesInvalidInputBeforeDispatch(t *testing.T) {
	sequence, calls := compensationFixture(t, func(context.Context, any) (any, error) {
		t.Error("dispatched")
		return nil, nil
	})
	ctx := testContext(t)
	subject := events.Subject("other")
	cases := map[string]func() error{
		"reserved position": func() error { _, err := sequence.Compensate(ctx, events.Unavailable, reopened{}); return err },
		"unregistered":      func() error { _, err := sequence.Compensate(ctx, 1, struct{ X int }{}); return err },
		"not compensation":  func() error { _, err := sequence.Compensate(ctx, 1, changed{}); return err },
		"route": func() error {
			_, err := sequence.Compensate(ctx, 1, reopened{}, eventsequences.WithRoute(eventsequences.Route{StreamID: "s"}))
			return err
		},
		"subject": func() error {
			_, err := sequence.Compensate(ctx, 1, reopened{}, eventsequences.WithSubject(subject))
			return err
		},
		"named tags": func() error {
			_, err := sequence.Compensate(ctx, 1, reopened{}, eventsequences.WithNamedTags(events.NamedTag{Name: "n", Value: "v"}))
			return err
		},
		"scope": func() error {
			_, err := sequence.Compensate(ctx, 1, reopened{}, eventsequences.WithScope(eventsequences.Scope{}))
			return err
		},
		"nil": func() error { _, err := sequence.Compensate(ctx, 1, reopened{}, nil); return err },
		"cancel": func() error {
			c, cancel := context.WithCancel(ctx)
			cancel()
			_, err := sequence.Compensate(c, 1, reopened{})
			return err
		},
	}
	for name, run := range cases {
		if err := run(); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if err := cases["not compensation"](); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Errorf("not compensation: %v", err)
	}
	if err := cases["unregistered"](); !errors.Is(err, faults.ErrNotRegistered) {
		t.Errorf("unregistered: %v", err)
	}
	if calls() != 0 {
		t.Fatalf("dispatched %d", calls())
	}
}

func TestCompensateTransportFailureIsOutcomeUnknownAndNotRetried(t *testing.T) {
	sequence, calls := compensationFixture(t, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.Unavailable, "gone")
	})
	_, err := sequence.Compensate(testContext(t), 2, reopened{"x"})
	var unknown *eventsequences.OutcomeUnknownError
	if !errors.As(err, &unknown) || calls() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls())
	}
}

func TestCompensationTargetTreatsZeroAsNone(t *testing.T) {
	if events.CompensationTarget(0) != nil {
		t.Fatal("zero must read as none")
	}
	if target := events.CompensationTarget(7); target == nil || *target != 7 {
		t.Fatalf("target=%v", target)
	}
}
