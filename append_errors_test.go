// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

func TestMalformedAppendResponsesFailClosed(t *testing.T) {
	for _, test := range []struct {
		name     string
		envelope *sequences.CommandResult_AppendResponse
	}{
		{"absent response", &sequences.CommandResult_AppendResponse{IsAuthorized: true}},
		{"unauthorized", &sequences.CommandResult_AppendResponse{AuthorizationFailureReason: "not allowed"}},
		{"validation", &sequences.CommandResult_AppendResponse{IsAuthorized: true, ValidationResults: []*sequences.ValidationResult{{Severity: sequences.ValidationResultSeverity_Error, Message: "bad input"}}}},
		{"success with errors", &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, Errors: []string{"failure"}, HasErrors: true}}},
		{"rejection without detail", &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{}}},
		{"reserved committed position", &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, SequenceNumber: ^uint64(0)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			kernel := &fakeKernel{append: func(context.Context, *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
				return test.envelope, nil
			}}
			client, _ := testClient(t, kernel)
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "customers")
			if err != nil {
				t.Fatal(err)
			}
			result, err := store.EventLog().Append(ctx, "source", CustomerRegistered{})
			if err == nil || result.Disposition == eventsequences.Committed {
				t.Fatalf("false success = %+v %v", result, err)
			}
		})
	}
}

func TestProtectedAppendCannotSilentlyDowngrade(t *testing.T) {
	kernel := &fakeKernel{append: func(_ context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		response := success(request, 1)
		response.Response.ConcurrencyCheckPerformed = false
		return response, nil
	}}
	client, _ := testClient(t, kernel)
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.EventLog().Append(ctx, "source", CustomerRegistered{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent()}))
	if !errors.Is(err, chronicle.ErrUnsupported) || result.Disposition != eventsequences.Committed {
		t.Fatalf("lost committed-but-unprotected disposition: %+v %v", result, err)
	}
}

func TestAppendValidationDoesNotDispatch(t *testing.T) {
	kernel := &fakeKernel{}
	client, _ := testClient(t, kernel)
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	other := events.SourceID("other")
	for _, option := range []eventsequences.AppendOption{nil, eventsequences.WithSubject(""), eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.Exact(events.Unavailable)}), eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.Exact(0), Filter: eventsequences.ScopeFilter{SourceID: &other}}), eventsequences.WithNamedTags(events.NamedTag{Name: " "})} {
		if _, err = store.EventLog().Append(ctx, "source", CustomerRegistered{}, option); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatalf("validation: %v", err)
		}
	}
	if _, err = store.EventLog().Append(ctx, "source", CustomerRenamed{}); !errors.Is(err, chronicle.ErrNotRegistered) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = store.EventLog().Append(canceled, "source", CustomerRegistered{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if kernel.appendCalls.Load() != 0 {
		t.Fatal("validation dispatched append")
	}
	if (eventsequences.AppendResult{}).Err() == nil {
		t.Fatal("zero result claims success")
	}
}

func TestCloseCancelsInFlightAppend(t *testing.T) {
	entered := make(chan struct{})
	kernel := &fakeKernel{append: func(ctx context.Context, _ *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	client, _ := testClient(t, kernel)
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan error, 1)
	go func() { _, err := store.EventLog().Append(ctx, "source", CustomerRegistered{}); completed <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	var unknown *eventsequences.OutcomeUnknownError
	select {
	case err = <-completed:
		if !errors.As(err, &unknown) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("append did not join")
	}
}
