// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	oc "github.com/cratis/chronicle.go/contracts/observation"
	sc "github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/observation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestOperationsTransportCancellationAfterDispatch(t *testing.T) {
	for _, mutation := range []bool{false, true} {
		t.Run(map[bool]string{false: "jobs read", true: "replay mutation"}[mutation], func(t *testing.T) {
			entered := make(chan struct{})
			exited := make(chan struct{})
			o, j := operationServices(t, operationsConnection(t, func(ctx context.Context, _ string, _ proto.Message) (proto.Message, error) {
				close(entered)
				<-ctx.Done()
				close(exited)
				return nil, status.FromContextError(ctx.Err()).Err()
			}))
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			result := make(chan error, 1)
			go func() {
				if mutation {
					_, err := o.Replay(ctx, "orders", "sequence")
					result <- err
				} else {
					_, err := j.List(ctx)
					result <- err
				}
			}()
			<-entered
			cancel()
			err := <-result
			<-exited
			var unknown *jobs.OutcomeUnknownError
			if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) != mutation {
				t.Fatal(err)
			}
		})
	}
}

func TestAppendCompletionUnavailableUnknownAndNotifications(t *testing.T) {
	ctx := testContext(t)
	definition, err := events.Define[CustomerRegistered](events.WithID("registered"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	type replyCase struct {
		name              string
		response          *sc.CommandResult_AppendResponse
		transport         error
		unknown, rejected bool
	}
	for _, tc := range []replyCase{
		{name: "committed zero", response: &sc.CommandResult_AppendResponse{IsAuthorized: true, Response: &sc.AppendResponse{IsSuccess: true}}},
		{name: "lost reply", transport: status.Error(codes.Unavailable, "lost"), unknown: true},
		{name: "known rejection", response: &sc.CommandResult_AppendResponse{IsAuthorized: false}, rejected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := operationsConnection(t, func(context.Context, string, proto.Message) (proto.Message, error) { return tc.response, tc.transport })
			seq, err := eventsequences.New("store", "tenant", "original", catalog, conn)
			if err != nil {
				t.Fatal(err)
			}
			var notification eventsequences.AppendNotification
			dispose := seq.OnAppend(func(n eventsequences.AppendNotification) { notification = n })
			defer dispose()
			appended, appendErr := seq.AppendWithMetadata(ctx, "source", CustomerRegistered{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
			if !tc.unknown && !tc.rejected && appendErr != nil {
				t.Fatal(appendErr)
			}
			c, err := appended.Completion()
			if tc.unknown {
				var unknown *eventsequences.OutcomeUnknownError
				if !errors.As(err, &unknown) {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			n, err := notification.Completion()
			if err != nil {
				t.Fatal(err)
			}
			var observers *observation.Service
			r, err := observers.WaitForCompletion(ctx, c, 0)
			if tc.rejected {
				if err != nil || !r.Trivial() {
					t.Fatal(r, err)
				}
			} else {
				var cannot *observation.CannotWaitError
				if !errors.As(err, &cannot) || cannot.Store != "store" || cannot.Sequence != "original" {
					t.Fatal(err)
				}
				if *n.Target().First != 0 || n.Target().EventTypeTails["registered"] != 0 {
					t.Fatal(n.Target())
				}
			}
		})
	}
	c, err := observation.NewCompletion("store", "tenant", "original", []events.TypeRef{{ID: "registered", Generation: 1}}, []events.SequenceNumber{events.Unavailable})
	if err != nil {
		t.Fatal(err)
	}
	r, err := (*observation.Service)(nil).WaitForCompletion(ctx, c, 0)
	if err != nil || !r.Trivial() {
		t.Fatal(r, err)
	}
}

func TestCompletionTimeoutRoundingAndInvalidCoordinates(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := observation.NewCompletion("store", "tenant", "original", []events.TypeRef{{ID: "registered", Generation: 1}}, []events.SequenceNumber{0})
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		o, _ := operationServices(t, inMemoryOperations{call: func(ctx context.Context, _ string, request proto.Message) (proto.Message, error) {
			calls++
			r := request.(*oc.WaitForObserverCompletionRequest)
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) != observation.ClientGrace+time.Nanosecond || r.TimeoutMilliseconds != 1 {
				t.Fatal(deadline, r)
			}
			return &oc.WaitForObserverCompletionResponse{IsSuccess: true}, nil
		}})
		if _, err := o.WaitForCompletion(t.Context(), c, time.Nanosecond); err != nil {
			t.Fatal(err)
		}
		for _, timeout := range []time.Duration{-2, time.Duration(1<<63 - 1)} {
			if _, err := o.WaitForCompletion(t.Context(), c, timeout); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatal(err)
			}
		}
		if calls != 1 {
			t.Fatal("invalid completion dispatched")
		}
		for _, tc := range []struct {
			types     []events.TypeRef
			positions []events.SequenceNumber
		}{{[]events.TypeRef{{ID: "a", Generation: 1}}, nil}, {[]events.TypeRef{{ID: "a"}}, []events.SequenceNumber{0}}, {[]events.TypeRef{{ID: "a", Generation: 1}}, []events.SequenceNumber{events.Unavailable - 1}}} {
			if _, err := observation.NewCompletion("store", "tenant", "original", tc.types, tc.positions); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatal(err)
			}
		}
	})
}
