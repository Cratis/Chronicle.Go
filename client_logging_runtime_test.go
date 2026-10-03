// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/reactors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type runtimeLoggingError struct{ calls *atomic.Int32 }

func (e *runtimeLoggingError) Error() string { e.calls.Add(1); return "secret-error" }
func (e *runtimeLoggingError) Format(s fmt.State, _ rune) {
	e.calls.Add(1)
	_, _ = io.WriteString(s, "secret-format")
}
func (e *runtimeLoggingError) Unwrap() error { e.calls.Add(1); return nil }
func (e *runtimeLoggingError) Is(error) bool { e.calls.Add(1); return false }
func (e *runtimeLoggingError) As(any) bool   { e.calls.Add(1); return false }
func (e *runtimeLoggingError) GRPCStatus() *status.Status {
	e.calls.Add(1)
	return status.New(codes.Internal, "secret-status")
}

type runtimeLoggingHandler struct {
	mu         sync.Mutex
	records    []slog.Record
	panicValue any
}

func (*runtimeLoggingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *runtimeLoggingHandler) Handle(_ context.Context, record slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, record.Clone())
	h.mu.Unlock() // Never hold the recorder lock across a caller panic/callback.
	if h.panicValue != nil {
		panic(h.panicValue)
	}
	return nil
}
func (h *runtimeLoggingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *runtimeLoggingHandler) WithGroup(string) slog.Handler      { return h }
func (h *runtimeLoggingHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}
func assertRuntimeDiagnostics(t *testing.T, h *runtimeLoggingHandler, stage string, count int) {
	t.Helper()
	found := 0
	for _, record := range h.snapshot() {
		if strings.Contains(record.Message, "secret") || record.NumAttrs() != 3 {
			t.Fatal("unsafe diagnostic message or attribute count")
		}
		record.Attrs(func(a slog.Attr) bool {
			if a.Key != "operation" && a.Key != "stage" && a.Key != "category" {
				t.Errorf("unexpected diagnostic attribute %s", a.Key)
			}
			if a.Value.Kind() != slog.KindString || strings.Contains(a.Value.String(), "secret") {
				t.Error("unsafe diagnostic attribute")
			}
			if a.Key == "stage" && a.Value.String() == stage {
				found++
			}
			return true
		})
	}
	if found != count {
		t.Fatalf("stage %s records = %d, want %d", stage, found, count)
	}
}

type runtimeLoggingMiddleware struct{ failure error }

func (runtimeLoggingMiddleware) Before(context.Context, reactors.Invocation) error  { return nil }
func (m runtimeLoggingMiddleware) After(context.Context, reactors.Invocation) error { return m.failure }

func TestClientLoggingAfterHookPanicPreservesRuntimeEffectAndCheckpoint(t *testing.T) {
	var hooks, handled atomic.Int32
	failure := &runtimeLoggingError{&hooks}
	h := &runtimeLoggingHandler{panicValue: failure}
	r := reactorRegistry(t)
	if err := chronicle.RegisterReactorHandlers(r, "secret-effects", []reactors.Handler{
		reactors.Returning(func(context.Context, ReactorInput) (ReactorOutput, error) {
			handled.Add(1)
			return ReactorOutput{42}, nil
		}),
	}, reactors.WithMiddleware(func() reactors.Middleware { return runtimeLoggingMiddleware{failure} })); err != nil {
		t.Fatal(err)
	}
	k := &reactorKernel{}
	committed := make(chan *sequences.AppendRequest, 1)
	k.append = func(_ context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		committed <- proto.Clone(request).(*sequences.AppendRequest)
		return success(request, 23), nil
	}
	waiting, _, clock := reactorRetryClock(t)
	client, _, ctx := reactorClient(t, k, r, clock, chronicle.WithLogger(slog.New(h)))
	s := receive(t, ctx, k.sessions)
	s.batches <- batch(7)
	result := receive(t, ctx, s.results)
	want := &contracts.ReactorResult{Partition: "source", State: contracts.ObservationState_Success, LastSuccessfulObservation: 7}
	if !proto.Equal(result, want) {
		t.Fatalf("ack = %v, want %v", result, want)
	}
	request := receive(t, ctx, committed)
	if request.Content != `{"number":42}` || request.EventSourceId != "source" || request.EventType.Id != "ReactorOutput" {
		t.Fatal("returned effect did not reach actual append RPC")
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseContext(ctx); err != nil {
		t.Fatal(err)
	}
	if handled.Load() != 1 || k.appendCalls.Load() != 1 || k.connectCalls.Load() != 1 || k.registrations.Load() != 1 {
		t.Fatal("logger panic retried effect or replaced generation")
	}
	select {
	case <-waiting:
		t.Fatal("logger panic triggered observer retry")
	default:
	}
	select {
	case <-k.sessions:
		t.Fatal("logger panic opened another observer")
	default:
	}
	assertRuntimeDiagnostics(t, h, "after", 1)
	if hooks.Load() != 0 {
		t.Fatal("application error or handler panic payload inspected")
	}
	// One controlled invocation only: no exactly-once claim about kernel recovery.
}

type runtimeLoggingReplayArtifact struct {
	failure           error
	notifications     *atomic.Int32
	handled, released *atomic.Int32
}

func (r *runtimeLoggingReplayArtifact) Handle(ReactorInput) { r.handled.Add(1) }
func (r *runtimeLoggingReplayArtifact) BeginReplay(context.Context) error {
	if r.notifications.Add(1) == 1 {
		return r.failure
	}
	return nil
}
func (*runtimeLoggingReplayArtifact) EndReplay(context.Context) error { return nil }
func (r *runtimeLoggingReplayArtifact) Close() error                  { r.released.Add(1); return nil }

func TestClientLoggingReplayFailureRetainsArtifactRouteAndGeneration(t *testing.T) {
	for _, override := range []bool{false, true} {
		for _, panicking := range []bool{false, true} {
			t.Run(fmt.Sprintf("override=%t/panicking=%t", override, panicking), func(t *testing.T) {
				old := slog.Default()
				t.Cleanup(func() { slog.SetDefault(old) })
				var hooks, notifications, handled, released atomic.Int32
				failure := &runtimeLoggingError{&hooks}
				captured, other, later := &runtimeLoggingHandler{}, &runtimeLoggingHandler{}, &runtimeLoggingHandler{}
				if panicking {
					captured.panicValue = failure
				}
				logger := slog.New(captured)
				slog.SetDefault(logger) // Custom before capture: not the pristine bridge.
				r := reactorRegistry(t)
				var options []reactors.Option
				var clientOptions []chronicle.ClientOption
				if override {
					options = append(options, reactors.WithLogger(logger)) // Equals the captured default, still explicit.
					clientOptions = append(clientOptions, chronicle.WithLogger(slog.New(other)))
				}
				if err := chronicle.RegisterReactor[*runtimeLoggingReplayArtifact](r, func() *runtimeLoggingReplayArtifact {
					return &runtimeLoggingReplayArtifact{failure, &notifications, &handled, &released}
				}, options...); err != nil {
					t.Fatal(err)
				}
				waiting, resume, clock := reactorRetryClock(t)
				clientOptions = append(clientOptions, clock)
				k := &reactorKernel{}
				client, _, ctx := reactorClient(t, k, r, clientOptions...)
				first := receive(t, ctx, k.sessions)
				slog.SetDefault(slog.New(later))
				first.batches <- &contracts.EventsToObserve{ReplayState: contracts.ReplayState_BeginReplay, Partition: "secret-partition"}
				receive(t, ctx, waiting)
				receive(t, ctx, first.done)
				assertRuntimeDiagnostics(t, captured, "replay", 1)
				assertRuntimeDiagnostics(t, captured, "resubscribe", 1)
				// The explicit client fallback may receive lifecycle records, but
				// never diagnostics from this overridden artifact.
				assertRuntimeDiagnostics(t, other, "replay", 0)
				assertRuntimeDiagnostics(t, other, "resubscribe", 0)
				if len(later.snapshot()) != 0 || released.Load() != 1 {
					t.Fatal("replay route changed or failed notification leaked artifact")
				}
				resume <- struct{}{}
				second := receive(t, ctx, k.sessions)
				if first.registration.ConnectionId != second.registration.ConnectionId || k.connectCalls.Load() != 1 || k.registrations.Load() != 1 {
					t.Fatal("notification failure replaced shared generation")
				}
				// A successful notifier and delivery on the resumed stream prove that
				// only the actual notifier failure, not logger panics, caused retry.
				second.batches <- &contracts.EventsToObserve{ReplayState: contracts.ReplayState_BeginReplay}
				second.batches <- batch(12)
				want := &contracts.ReactorResult{Partition: "source", State: contracts.ObservationState_Success, LastSuccessfulObservation: 12}
				if result := receive(t, ctx, second.results); !proto.Equal(result, want) {
					t.Fatalf("resumed ack = %v, want %v", result, want)
				}
				if err := client.CloseContext(ctx); err != nil {
					t.Fatal(err)
				}
				if notifications.Load() != 2 || handled.Load() != 1 || released.Load() != 3 || hooks.Load() != 0 || k.appendCalls.Load() != 0 {
					t.Fatal("logger altered replay outcome, inspected failure, or leaked artifact")
				}
				select {
				case <-waiting:
					t.Fatal("logging caused additional retry")
				default:
				}
				select {
				case <-first.results:
					t.Fatal("acknowledged replay notification")
				default:
				}
				if len(later.snapshot()) != 0 {
					t.Fatal("captured custom handler rerouted to later default")
				}
			})
		}
	}
}
