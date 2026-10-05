// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

// acceptedHistoryConnection is an offline protocol fixture, not a kernel or a
// storage substitute. It acknowledges requests without applying any mutations.
type acceptedHistoryConnection struct{}

func (acceptedHistoryConnection) Invoke(_ context.Context, method string, _, reply any, _ ...grpc.CallOption) error {
	switch method {
	case sequences.EventSequences_Redact_FullMethodName, sequences.EventSequences_RedactForEventSource_FullMethodName, sequences.EventSequences_Revise_FullMethodName:
		reply.(*sequences.CommandResult).IsAuthorized = true
	case sequences.EventSequences_CompleteStream_FullMethodName:
		result := reply.(*sequences.CommandResult_CompleteStreamResponse)
		result.IsAuthorized = true
		result.Response = &sequences.CompleteStreamResponse{IsSuccess: true, SequenceNumber: 12}
	default:
		return fmt.Errorf("unexpected method %s", method)
	}
	return nil
}

func (acceptedHistoryConnection) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected stream")
}

func ExampleSequence_Redact() {
	type OrderNamed struct{ Name string }
	definition, err := events.Define[OrderNamed]()
	if err != nil {
		panic(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		panic(err)
	}
	// In production use store.EventLog(), after authorization and approval.
	sequence, err := eventsequences.New("orders", "Default", events.EventLog, catalog, acceptedHistoryConnection{})
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.WithIdentity(ctx, identities.Identity{Subject: "approved-operator"})
	ctx = metadata.WithCausation(ctx, metadata.Causation{Occurred: time.Now().UTC(), Type: "history-correction", Properties: map[string]string{"ticket": "case-40"}})
	if err = sequence.Revise(ctx, 12, OrderNamed{Name: "corrected non-personal label"}); err != nil {
		panic(err)
	}
	if err = sequence.Redact(ctx, 12, "approved removal"); err != nil {
		var unknown *eventsequences.MutationOutcomeUnknownError
		if errors.As(err, &unknown) {
			fmt.Println("reconcile; do not retry")
		}
		return
	}
	fmt.Println("redaction request accepted; application and replay may still be pending")
	// This explicitly targets every generation of OrderNamed for order-42,
	// across all routes. Omitting the type argument targets ALL event types.
	if err = sequence.RedactForEventSource(ctx, "order-42", "approved source cleanup", definition.Ref().ID); err != nil {
		panic(err)
	}
	// Stream completion is a separate synchronous operation, not source deletion.
	if _, err = sequence.CompleteStream(ctx, "Orders", "closed-order"); err != nil {
		panic(err)
	}
	fmt.Println("stream closed")
	// Output:
	// redaction request accepted; application and replay may still be pending
	// stream closed
}
