// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsources_test

import (
	"context"
	"errors"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/eventsources"
	"github.com/cratis/chronicle.go/eventsources"
	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type recorder struct {
	grpc.ClientConnInterface
	requests []*contracts.RegisterEventSourcesRequest
	reply    *contracts.CommandResult
	err      error
}

func (r *recorder) Invoke(_ context.Context, _ string, args, reply any, _ ...grpc.CallOption) error {
	r.requests = append(r.requests, proto.Clone(args.(proto.Message)).(*contracts.RegisterEventSourcesRequest))
	if r.err != nil {
		return r.err
	}
	proto.Merge(reply.(proto.Message), r.reply)
	return nil
}

func newService(t *testing.T, conn *recorder) *eventsources.Service {
	t.Helper()
	service, err := eventsources.New("store", conn)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestRegisterSendsClientOwnedDefinitionsInOrder(t *testing.T) {
	conn := &recorder{reply: &contracts.CommandResult{IsAuthorized: true}}
	streams := []eventsources.Stream{{Name: "b", Concurrency: eventsources.EventStreamID}, {Name: "a", Description: "first"}}
	definition := eventsources.Definition{Name: "Billing", Description: "d", Concurrency: eventsources.EventSourceID | eventsources.EventSourceType, Streams: streams}
	if err := newService(t, conn).Register(context.Background(), definition); err != nil {
		t.Fatal(err)
	}
	want := &contracts.RegisterEventSourcesRequest{EventStore: "store", Sources: []*contracts.EventSourceDefinition{{
		Name: "Billing", Description: "d", Owner: contracts.EventSourceOwner_Client, Concurrency: 3,
		Streams: []*contracts.EventStreamDefinition{{Name: "b", Concurrency: 8}, {Name: "a", Description: "first"}},
	}}}
	if len(conn.requests) != 1 || !proto.Equal(conn.requests[0], want) {
		t.Fatalf("request = %v, want %v", conn.requests, want)
	}
}

func TestRegisterRejectsInvalidInputBeforeAnyRPC(t *testing.T) {
	cases := map[string][]eventsources.Definition{
		"blank name":       {{Name: " "}},
		"padded source":    {{Name: " A"}},
		"duplicate source": {{Name: "A"}, {Name: "A"}},
		"blank stream":     {{Name: "A", Streams: []eventsources.Stream{{Name: ""}}}},
		"padded stream":    {{Name: "A", Streams: []eventsources.Stream{{Name: "s "}}}},
		"duplicate stream": {{Name: "A", Streams: []eventsources.Stream{{Name: "s"}, {Name: "s"}}}},
		"unknown flags":    {{Name: "A", Concurrency: 16}},
		"unknown stream":   {{Name: "A", Streams: []eventsources.Stream{{Name: "s", Concurrency: -1}}}},
	}
	for name, definitions := range cases {
		t.Run(name, func(t *testing.T) {
			conn := &recorder{reply: &contracts.CommandResult{IsAuthorized: true}}
			err := newService(t, conn).Register(context.Background(), definitions...)
			if !errors.Is(err, faults.ErrInvalidConfiguration) || len(conn.requests) != 0 {
				t.Fatalf("err = %v, requests = %d", err, len(conn.requests))
			}
		})
	}
}

func TestRegisterEmptyCallSendsNothingUnlessCanceled(t *testing.T) {
	conn := &recorder{}
	service := newService(t, conn)
	if err := service.Register(context.Background()); err != nil || len(conn.requests) != 0 {
		t.Fatalf("err = %v, requests = %d", err, len(conn.requests))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.Register(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestRegisterEnvelopeFailureIsNotSuccess(t *testing.T) {
	conn := &recorder{reply: &contracts.CommandResult{IsAuthorized: false}}
	if err := newService(t, conn).Register(context.Background(), eventsources.Definition{Name: "A"}); err == nil {
		t.Fatal("unauthorized reply reported success")
	}
}

func TestObserveIsUnsupported(t *testing.T) {
	if err := newService(t, &recorder{}).Observe(context.Background(), nil); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("err = %v", err)
	}
}
