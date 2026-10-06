// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type statsRPCKey struct{}
type statsConnectionKey struct{}

type recordingStatsHandler struct {
	connections atomic.Int32
	rpcs        atomic.Int32
	closed      atomic.Bool
	begins      chan int32
	ends        chan string
}

func (h *recordingStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, statsConnectionKey{}, h.connections.Add(1))
}
func (h *recordingStatsHandler) HandleConn(ctx context.Context, event stats.ConnStats) {
	if _, ok := event.(*stats.ConnBegin); ok && h.begins != nil {
		h.begins <- ctx.Value(statsConnectionKey{}).(int32)
	}
}
func (h *recordingStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	h.rpcs.Add(1)
	return context.WithValue(ctx, statsRPCKey{}, info.FullMethodName)
}
func (h *recordingStatsHandler) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if _, ok := event.(*stats.End); ok && h.ends != nil {
		h.ends <- ctx.Value(statsRPCKey{}).(string)
	}
}
func (h *recordingStatsHandler) Close() error { h.closed.Store(true); return nil }

func TestGRPCStatsHandlerValidationAndLastWins(t *testing.T) {
	_, _, borrowed := boundsServer(t)
	var typedNil *recordingStatsHandler
	valid := &recordingStatsHandler{}
	for name, options := range map[string][]ClientOption{
		"nil":            {WithGRPCStatsHandler(nil)},
		"typed nil":      {WithGRPCStatsHandler(typedNil)},
		"final nil":      {WithGRPCStatsHandler(valid), WithGRPCStatsHandler(nil)},
		"borrowed first": {WithGRPCConnection(borrowed), WithGRPCStatsHandler(valid)},
		"borrowed last":  {WithGRPCStatsHandler(valid), WithGRPCConnection(borrowed)},
	} {
		t.Run(name, func(t *testing.T) {
			client, err := NewClient(append(options, WithNoAuthentication())...)
			if client != nil || !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("client=%v error=%v", client, err)
			}
		})
	}
	client, err := NewClient(WithGRPCStatsHandler(nil), WithGRPCStatsHandler(valid))
	if err != nil {
		t.Fatal(err)
	}
	if client.config.grpcStatsHandler != valid || valid.connections.Load() != 0 || valid.rpcs.Load() != 0 {
		t.Fatal("final handler not captured without I/O")
	}
	if err := client.Close(); err != nil || valid.closed.Load() {
		t.Fatalf("borrowed handler closed: %v", err)
	}
}

func TestGRPCStatsHandlerOwnedGenerationsUnaryAndStream(t *testing.T) {
	address, policy, _ := boundsServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	first := &recordingStatsHandler{}
	handler := &recordingStatsHandler{begins: make(chan int32, 8), ends: make(chan string, 32)}
	client, err := Dial(ctx, WithConnectionString("chronicle://"+address), WithTLS(policy),
		WithNoAuthentication(), WithSkipKeepAlive(), WithSkipCompatibilityCheck(),
		WithGRPCStatsHandler(first), WithGRPCStatsHandler(handler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, generation := range []int32{1, 2} {
		if generation == 2 {
			client.mu.Lock()
			old := client.current
			old.cancel()
			client.mu.Unlock()
			if err := client.Ready(ctx); err != nil {
				t.Fatal(err)
			}
			client.mu.Lock()
			replaced := client.current.number > old.number && client.current.raw != old.raw
			client.mu.Unlock()
			if !replaced {
				t.Fatal("owned connection was not replaced")
			}
		}
		select {
		case got := <-handler.begins:
			if got != generation {
				t.Fatalf("connection=%d want=%d", got, generation)
			}
		case <-ctx.Done():
			t.Fatal("handler did not observe owned connection", ctx.Err())
		}
		for _, streaming := range []bool{false, true} {
			if err := boundsCall(ctx, client.transport, streaming, wrapperspb.String("small")); err != nil {
				t.Fatal(err)
			}
			want := "/bounds.Echo/Unary"
			if streaming {
				want = "/bounds.Echo/Stream"
			}
			observed := false
			for !observed {
				select {
				case got := <-handler.ends:
					observed = got == want
				case <-ctx.Done():
					t.Fatal("handler did not observe RPC end", want, ctx.Err())
				}
			}
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if first.connections.Load() != 0 || first.rpcs.Load() != 0 || handler.closed.Load() || first.closed.Load() {
		t.Fatal("last-wins or borrowed handler ownership violated")
	}
}
