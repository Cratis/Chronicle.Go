// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/seeding"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type routedSeedKernel struct {
	seedKernel
	probes  atomic.Int32
	support func(context.Context) (*contracts.QueryResult_EventSeedingSupportResponse, error)
}

func (k *routedSeedKernel) GetSeedingSupport(ctx context.Context, _ *emptypb.Empty) (*contracts.QueryResult_EventSeedingSupportResponse, error) {
	k.probes.Add(1)
	if k.support != nil {
		return k.support(ctx)
	}
	return seedRoutingSupport(true), nil
}

func seedRoutingSupport(supported bool) *contracts.QueryResult_EventSeedingSupportResponse {
	return &contracts.QueryResult_EventSeedingSupportResponse{IsAuthorized: true, Data: &contracts.EventSeedingSupportResponse{RoutingSupported: supported}}
}

func TestSeedingRoutingProbesOnlyNonDefaultBatchesBeforeSend(t *testing.T) {
	cases := []struct {
		name          string
		typ           events.StreamType
		id            events.StreamID
		ns            Namespace
		legacy, empty bool
		probes        int32
	}{
		{name: "legacy", legacy: true},
		{name: "explicit defaults", typ: events.AllStreamTypes, id: events.DefaultStreamID},
		{name: "empty defaults"},
		{name: "empty routed batch", typ: "Orders", id: "one", empty: true},
		{name: "type", typ: "Orders", probes: 1},
		{name: "ID only", id: "one", probes: 1},
		{name: "namespace only", id: "one", ns: "red", probes: 1},
		{name: "whitespace preserved", typ: " ", id: " ", probes: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kernel := &routedSeedKernel{}
			kernel.send = func(_ context.Context, request *contracts.SeedEventsRequest) (*contracts.CommandResult, error) {
				if kernel.probes.Load() != tc.probes {
					t.Error("send before required capability query")
				}
				return &contracts.CommandResult{IsAuthorized: true}, nil
			}
			registry := seedRegistry(t, func(b *seeding.Builder) error {
				if !tc.legacy {
					b = b.WithStream(tc.typ, tc.id)
				}
				if tc.ns != "" {
					b = b.ForNamespace(tc.ns)
				}
				if !tc.empty {
					seeding.For(b, "one", lifecycleEvent{})
				}
				return nil
			})
			client, ctx := supervisionClient(t, &supervisedKernel{seeding: kernel}, WithRegistry(registry), seedPolicy())
			if _, err := client.EventStore(ctx, "store"); err != nil {
				t.Fatal(err)
			}
			if kernel.probes.Load() != tc.probes {
				t.Fatal("probe count", kernel.probes.Load())
			}
		})
	}
}

func TestSeedingRoutingRefusalSendsNoPartOfMixedBatch(t *testing.T) {
	cases := []struct {
		name     string
		response *contracts.QueryResult_EventSeedingSupportResponse
		err      error
		want     error
		code     codes.Code
	}{
		{name: "false", response: seedRoutingSupport(false), want: ErrUnsupported},
		{name: "unimplemented", err: status.Error(codes.Unimplemented, "old kernel"), want: ErrUnsupported},
		{name: "missing data", response: &contracts.QueryResult_EventSeedingSupportResponse{IsAuthorized: true}, want: ErrProtocol},
		{name: "authorization", response: &contracts.QueryResult_EventSeedingSupportResponse{Data: &contracts.EventSeedingSupportResponse{RoutingSupported: true}}},
		{name: "validation", response: &contracts.QueryResult_EventSeedingSupportResponse{IsAuthorized: true, ValidationResults: []*contracts.ValidationResult{{Severity: contracts.ValidationResultSeverity(3), Message: "invalid"}}, Data: &contracts.EventSeedingSupportResponse{RoutingSupported: true}}},
		{name: "exception", response: &contracts.QueryResult_EventSeedingSupportResponse{IsAuthorized: true, ExceptionMessages: []string{"failed"}, Data: &contracts.EventSeedingSupportResponse{RoutingSupported: true}}},
		{name: "transport", err: status.Error(codes.Unavailable, "try later"), code: codes.Unavailable},
		{name: "permission status", err: status.Error(codes.PermissionDenied, "refused"), code: codes.PermissionDenied},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kernel := &routedSeedKernel{support: func(context.Context) (*contracts.QueryResult_EventSeedingSupportResponse, error) {
				return tc.response, tc.err
			}}
			registry := seedRegistry(t, mixedRoutedSeeds)
			client, ctx := supervisionClient(t, &supervisedKernel{seeding: kernel}, WithRegistry(registry), seedPolicy())
			_, err := client.EventStore(ctx, "store")
			assertSeedProbeError(t, err, tc.want, tc.code)
			if kernel.calls.Load() != 0 || kernel.probes.Load() != 1 {
				t.Fatal("refused batch sent entries or repeated probe", kernel.calls.Load(), kernel.probes.Load())
			}
		})
	}
}

func mixedRoutedSeeds(b *seeding.Builder) error {
	seeding.For(b, "legacy", lifecycleEvent{})
	seeding.For(b.ForNamespace("red").WithStream("Orders", "one"), "routed", lifecycleEvent{})
	return nil
}

func assertSeedProbeError(t *testing.T, err, want error, code codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatal("probe failure accepted")
	}
	switch {
	case want != nil:
		assertSeedProbeSentinel(t, err, want)
	case code != codes.OK:
		assertSeedProbeStatus(t, err, code)
	default:
		assertSeedProbeEnvelope(t, err)
	}
}

func assertSeedProbeSentinel(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatal(err)
	}
	if errors.Is(want, ErrUnsupported) && !strings.Contains(err.Error(), "kernel") {
		t.Fatal("unsupported error lacks kernel action", err)
	}
}

func assertSeedProbeStatus(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatal(err)
	}
}

func assertSeedProbeEnvelope(t *testing.T, err error) {
	t.Helper()
	var envelope *EnvelopeError
	if !errors.As(err, &envelope) || errors.Is(err, ErrUnsupported) {
		t.Fatal("envelope error lost", err)
	}
}

func assertNoSeedError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeedingRoutingChecksEveryIndexAndSourceType(t *testing.T) {
	entry := &contracts.SeedingEntry{EventSourceType: string(events.DefaultSourceType), EventStreamType: string(events.AllStreamTypes), EventStreamId: string(events.DefaultStreamID)}
	byType := []*contracts.EventTypeSeedEntries{{Entries: []*contracts.SeedingEntry{entry}}}
	bySource := []*contracts.EventSourceSeedEntries{{Entries: []*contracts.SeedingEntry{entry}}}
	requests := []*contracts.SeedEventsRequest{
		{GlobalByEventType: byType},
		{GlobalByEventSource: bySource},
		{NamespacedEntries: []*contracts.NamespacedSeedEntries{{ByEventType: byType}}},
		{NamespacedEntries: []*contracts.NamespacedSeedEntries{{ByEventSource: bySource}}},
	}
	for _, request := range requests {
		if seedRequestNeedsRouting(request) {
			t.Fatal("default source/stream route needs capability", request)
		}
	}
	entry.EventSourceType = " "
	for _, request := range requests {
		if !seedRequestNeedsRouting(request) {
			t.Fatal("non-default source type missed", request)
		}
	}
}

func TestSeedingRoutingNilSupportFailsProtocol(t *testing.T) {
	if err := checkSeedRoutingSupport(nil, nil); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestSeedingRoutingManualRetryRetainsPreparationAndConsumesSuccess(t *testing.T) {
	var supported atomic.Bool
	kernel := &routedSeedKernel{support: func(context.Context) (*contracts.QueryResult_EventSeedingSupportResponse, error) {
		return seedRoutingSupport(supported.Load()), nil
	}}
	registry := seedRegistry(t, func(*seeding.Builder) error { return nil })
	client, ctx := supervisionClient(t, &supervisedKernel{seeding: kernel}, WithRegistry(registry), seedPolicy())
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	var prepared int
	batch, err := store.PrepareSeeds(seeding.Func(func(b *seeding.Builder) error { prepared++; return mixedRoutedSeeds(b) }))
	if err != nil {
		t.Fatal(err)
	}
	frozen := batch.definition.Contract("store")
	if err := batch.Register(ctx); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	if kernel.calls.Load() != 0 {
		t.Fatal("unsupported batch sent")
	}
	supported.Store(true)
	kernel.send = func(_ context.Context, request *contracts.SeedEventsRequest) (*contracts.CommandResult, error) {
		if !proto.Equal(request, frozen) {
			t.Error("retry changed prepared routing", request)
		}
		return &contracts.CommandResult{IsAuthorized: true}, nil
	}
	assertNoSeedError(t, batch.Register(ctx))
	assertNoSeedError(t, batch.Register(ctx))
	if got := [3]int{prepared, int(kernel.probes.Load()), int(kernel.calls.Load())}; got != [3]int{1, 2, 1} {
		t.Fatal("manual retry or consumption changed", prepared, kernel.probes.Load(), kernel.calls.Load())
	}
}

func TestSeedingRoutingReconnectReprobesFrozenBatch(t *testing.T) {
	var prepared atomic.Int32
	requests := make(chan *contracts.SeedEventsRequest, 8)
	kernel := &routedSeedKernel{}
	kernel.send = func(_ context.Context, request *contracts.SeedEventsRequest) (*contracts.CommandResult, error) {
		requests <- request
		return &contracts.CommandResult{IsAuthorized: true}, nil
	}
	registry := seedRegistry(t, func(b *seeding.Builder) error { prepared.Add(1); return mixedRoutedSeeds(b) })
	supervisor := &supervisedKernel{seeding: kernel}
	client, ctx := supervisionClient(t, supervisor, WithRegistry(registry), seedPolicy())
	if _, err := client.EventStore(ctx, "store"); err != nil {
		t.Fatal(err)
	}
	first := receiveSeed(t, ctx, requests)
	supervisor.endStream <- status.Error(codes.Unavailable, "replace generation")
	second := receiveSeed(t, ctx, requests)
	if !proto.Equal(first, second) {
		t.Fatal("reconnect changed batch")
	}
	if got := [3]int32{prepared.Load(), kernel.probes.Load(), supervisor.appends.Load()}; got != [3]int32{1, 2, 0} {
		t.Fatal("reconnect cached support, reran preparation or appended", got)
	}
}

func TestSeedingRoutingProbeHonorsCancellationAndDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[deadline], func(t *testing.T) {
			entered := make(chan struct{}, 1)
			kernel := &routedSeedKernel{support: func(ctx context.Context) (*contracts.QueryResult_EventSeedingSupportResponse, error) {
				entered <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}}
			client, ctx := supervisionClient(t, &supervisedKernel{seeding: kernel}, WithRegistry(seedRegistry(t, func(*seeding.Builder) error { return nil })), seedPolicy())
			store, err := client.EventStore(ctx, "store")
			if err != nil {
				t.Fatal(err)
			}
			batch, err := store.PrepareSeeds(seeding.Func(mixedRoutedSeeds))
			if err != nil {
				t.Fatal(err)
			}
			attempt, cancel := seedProbeContext(ctx, deadline)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- batch.Register(attempt) }()
			awaitSignal(t, ctx, entered)
			if !deadline {
				cancel()
			}
			awaitSeedProbeError(t, ctx, attempt, result)
			if kernel.calls.Load() != 0 {
				t.Fatal("canceled probe sent entries")
			}
		})
	}
}

func awaitSeedProbeError(t *testing.T, ctx, attempt context.Context, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		assertSeedProbeStatus(t, err, status.FromContextError(attempt.Err()).Code())
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func seedProbeContext(ctx context.Context, deadline bool) (context.Context, context.CancelFunc) {
	if deadline {
		return context.WithTimeout(ctx, 250*time.Millisecond)
	}
	return context.WithCancel(ctx)
}
