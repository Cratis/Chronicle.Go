// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	oc "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/observation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestObserverRemovalUsesAuthoritativeCustomSequence(t *testing.T) {
	for _, mode := range []string{"derived", "explicit", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			mutations := 0
			o, _ := operationServices(t, operationsConnection(t, func(_ context.Context, method string, request proto.Message) (proto.Message, error) {
				if methodName(method) == "GetObservers" {
					return &oc.IEnumerable_ObserverInformation{Items: []*oc.ObserverInformation{operationObserver()}}, nil
				}
				mutations++
				equalOperation(t, request, &oc.RemoveObserver{EventStore: "store", Namespace: "tenant", ObserverId: "orders", EventSequenceId: "source-sequence"})
				return &oc.RemoveObserverResponse{Outcome: oc.ObserverRemovalOutcome_ObserverSubscribed, BlockingNamespace: "tenant"}, nil
			}))
			var result observation.RemovalResult
			var err error
			ctx := testContext(t)
			switch mode {
			case "derived":
				result, err = o.Remove(ctx, "orders")
			case "explicit":
				result, err = o.RemoveFrom(ctx, "orders", "source-sequence")
			case "snapshot":
				all, listErr := o.List(ctx)
				if listErr != nil {
					t.Fatal(listErr)
				}
				result, err = all[0].Remove(ctx)
			}
			if err != nil || mutations != 1 || result.Outcome != observation.ObserverSubscribed {
				t.Fatal(result, err, mutations)
			}
		})
	}
}

func TestObserverRemovalFailsBeforeMutationWithoutSafeTarget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		items     []*oc.ObserverInformation
		readError error
		explicit  bool
		want      error
	}{
		{name: "absent", want: chronicle.ErrProtocol},
		{name: "ambiguous", items: []*oc.ObserverInformation{operationObserver(), operationObserver()}, want: chronicle.ErrProtocol},
		{name: "malformed", items: []*oc.ObserverInformation{{Id: "orders"}}, want: chronicle.ErrProtocol},
		{name: "wrong sequence", items: []*oc.ObserverInformation{operationObserver()}, explicit: true, want: chronicle.ErrInvalidConfiguration},
		{name: "unsupported", readError: status.Error(codes.Unimplemented, "no list")},
		{name: "unavailable", readError: status.Error(codes.Unavailable, "unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutations := 0
			o, _ := operationServices(t, operationsConnection(t, func(_ context.Context, method string, _ proto.Message) (proto.Message, error) {
				if methodName(method) != "GetObservers" {
					mutations++
					return &oc.RemoveObserverResponse{}, nil
				}
				return &oc.IEnumerable_ObserverInformation{Items: tc.items}, tc.readError
			}))
			var err error
			if tc.explicit {
				_, err = o.RemoveFrom(testContext(t), "orders", "event-log")
			} else {
				_, err = o.Remove(testContext(t), "orders")
			}
			var unknown *observation.OutcomeUnknownError
			if err == nil || mutations != 0 || errors.As(err, &unknown) || (tc.want != nil && !errors.Is(err, tc.want)) || (tc.readError != nil && status.Code(err) != status.Code(tc.readError)) {
				t.Fatal(err, mutations)
			}
		})
	}
}

func TestObserverSnapshotRemovalRejectsChangedSequence(t *testing.T) {
	calls, mutations := 0, 0
	o, _ := operationServices(t, inMemoryOperations{call: func(_ context.Context, method string, _ proto.Message) (proto.Message, error) {
		if methodName(method) != "GetObservers" {
			mutations++
			return &oc.RemoveObserverResponse{}, nil
		}
		calls++
		v := operationObserver()
		if calls > 1 {
			v.EventSequenceId = "replacement-sequence"
		}
		return &oc.IEnumerable_ObserverInformation{Items: []*oc.ObserverInformation{v}}, nil
	}})
	all, err := o.List(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	_, err = all[0].Remove(testContext(t))
	if !errors.Is(err, chronicle.ErrInvalidConfiguration) || mutations != 0 {
		t.Fatal(err, mutations)
	}
}
