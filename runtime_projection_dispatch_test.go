// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type definitionRaw struct {
	call func(context.Context, any) error
}

func (r definitionRaw) Invoke(ctx context.Context, _ string, _, reply any, _ ...grpc.CallOption) error {
	return r.call(ctx, reply)
}
func (definitionRaw) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, ErrUnsupported
}

func TestRuntimeProjectionFalseWirePresence(t *testing.T) {
	data, err := (projectionRegistrationCodec{}).Marshal(&contracts.RegisterRequest{FullSet: false})
	if err != nil || !bytes.Equal(data, []byte{0x20, 0x00}) {
		t.Fatalf("false presence = %x %v", data, err)
	}
	data, err = (projectionRegistrationCodec{}).Marshal(&contracts.RegisterRequest{FullSet: true})
	if err != nil || !bytes.Equal(data, []byte{0x20, 0x01}) {
		t.Fatalf("true presence = %x %v", data, err)
	}
}

func TestRuntimeDestructiveDispositionIsRecordedBeforeFlightRelease(t *testing.T) {
	cases := []struct {
		name    string
		call    func(context.CancelFunc, *emptypb.Empty) error
		unknown bool
	}{
		{"acknowledged", func(context.CancelFunc, *emptypb.Empty) error { return nil }, false},
		{"acknowledged-before-cancel", func(cancel context.CancelFunc, _ *emptypb.Empty) error { cancel(); return nil }, false},
		{"terminal-rejection", func(context.CancelFunc, *emptypb.Empty) error { return status.Error(codes.InvalidArgument, "rejected") }, false},
		{"before-dispatch", func(context.CancelFunc, *emptypb.Empty) error { return &faults.BeforeDispatch{Cause: context.Canceled} }, false},
		{"deadline", func(context.CancelFunc, *emptypb.Empty) error { return status.Error(codes.DeadlineExceeded, "unknown") }, true},
		{"cancel", func(cancel context.CancelFunc, _ *emptypb.Empty) error { cancel(); return context.Canceled }, true},
		{"malformed", func(_ context.CancelFunc, ack *emptypb.Empty) error {
			ack.ProtoReflect().SetUnknown([]byte{8, 1})
			return nil
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := memoryLifecycleClient(t, nil)
			store := testDefinitionStore(t, &EventStore{client: client, name: "store", namespace: DefaultNamespace})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			g := &generation{ctx: t.Context()}
			g.raw = definitionRaw{call: func(_ context.Context, reply any) error {
				client.mu.Lock()
				flight := store.definitions.flight
				client.mu.Unlock()
				if !flight {
					t.Error("raw dispatch outside flight")
				}
				return tc.call(cancel, reply.(*emptypb.Empty))
			}}
			g.transport = &generationTransport{generation: g}
			_ = store.definitionTransport(g, store.definitionRoot(), true).Invoke(ctx, "register", nil, &emptypb.Empty{})
			client.mu.Lock()
			flight, unknown := store.definitions.flight, store.definitions.destructiveUnknown
			client.mu.Unlock()
			if flight || unknown != tc.unknown {
				t.Fatalf("flight=%v unknown=%v", flight, unknown)
			}
			// A later acknowledgement must never clear a prior unknown.
			g.raw = definitionRaw{call: func(context.Context, any) error { return nil }}
			if err := store.definitionTransport(g, store.definitionRoot(), true).Invoke(t.Context(), "register", nil, &emptypb.Empty{}); err != nil {
				t.Fatal(err)
			}
			if store.definitions.destructiveUnknown != tc.unknown {
				t.Fatal("later ack reset latch")
			}
		})
	}
}

func TestRuntimeQueuedFullSetSupersededBeforeAndInsideAuthorization(t *testing.T) {
	for _, inside := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "inside"}[inside], func(t *testing.T) {
			registry, _ := projectionRegistry(t)
			client, ctx := supervisionClient(t, runtimeKernel(), WithRegistry(registry))
			store, err := client.EventStore(ctx, "store")
			if err != nil {
				t.Fatal(err)
			}
			root := store.definitionRoot()
			entered, release := make(chan struct{}), make(chan struct{})
			var raw atomic.Int32
			g := &generation{ctx: ctx, raw: definitionRaw{call: func(context.Context, any) error { raw.Add(1); return nil }}, tokens: decisionTokenSource(func(context.Context) (Token, error) {
				close(entered)
				<-release
				return Token{AccessToken: "token"}, nil
			})}
			g.transport = &generationTransport{generation: g}
			completed := make(chan error, 1)
			invoke := func() {
				completed <- store.definitionTransport(g, root, true).Invoke(ctx, "register", nil, &emptypb.Empty{})
			}
			if inside {
				go invoke()
				awaitSignal(t, ctx, entered)
			}
			_, declaration := runtimeModel(t)
			if result, err := store.RegisterProjection(ctx, declaration); err != nil || !result.Published {
				t.Fatal(result, err)
			}
			if !inside {
				go invoke()
				awaitSignal(t, ctx, entered)
			}
			close(release)
			if err := <-completed; !errors.Is(err, errDefinitionSuperseded) || raw.Load() != 0 {
				t.Fatal("stale raw dispatch", err, raw.Load())
			}
		})
	}
}

func TestRuntimeAddWaitsForDestructiveFlightAndCancellationDoesNotPublish(t *testing.T) {
	registry, _ := projectionRegistry(t)
	client, ctx := supervisionClient(t, runtimeKernel(), WithRegistry(registry))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	g := &generation{ctx: ctx, raw: definitionRaw{call: func(context.Context, any) error { close(entered); <-release; return nil }}}
	g.transport = &generationTransport{generation: g}
	done := make(chan error, 1)
	go func() {
		done <- store.definitionTransport(g, store.definitionRoot(), true).Invoke(ctx, "register", nil, &emptypb.Empty{})
	}()
	awaitSignal(t, ctx, entered)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, declaration := runtimeModel(t)
	result, err := store.RegisterProjection(canceled, declaration)
	if result.Published || !errors.Is(err, context.Canceled) {
		t.Fatal(result, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if result, err := store.RegisterProjection(ctx, declaration); err != nil || !result.Published {
		t.Fatal(result, err)
	}
}
