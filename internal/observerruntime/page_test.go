// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	reducercontracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"google.golang.org/grpc"
)

type PageInput struct{ Number int }
type PageNote struct{ Number int }

// pageRecorder records calls; methods share it across activations.
type pageRecorder struct {
	mu      sync.Mutex
	calls   []string
	batches [][]events.SequenceNumber
	context []observation.BatchContext
	failAt  int
	batch   func(*observation.EventBatch) error
}

func (r *pageRecorder) record(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

type PerEventReactor struct{ r *pageRecorder }

func (h *PerEventReactor) Input(e PageInput, ec events.Context) error {
	h.r.record(fmt.Sprintf("%s:%d", ec.SourceID, ec.SequenceNumber))
	if e.Number == h.r.failAt {
		return errors.New("handler failure")
	}
	return nil
}

type MixedReactor struct{ r *pageRecorder }

func (h *MixedReactor) Index(_ context.Context, batch *observation.EventBatch, bc observation.BatchContext) error {
	var numbers []events.SequenceNumber
	for _, item := range batch.All() {
		if _, ok := item.Event.(PageInput); !ok {
			return errors.New("batch received an event it does not observe")
		}
		numbers = append(numbers, item.Context.SequenceNumber)
	}
	h.r.mu.Lock()
	h.r.batches = append(h.r.batches, numbers)
	h.r.context = append(h.r.context, bc)
	h.r.mu.Unlock()
	if h.r.batch != nil {
		return h.r.batch(batch)
	}
	return nil
}
func (h *MixedReactor) Note(_ PageNote, ec events.Context) {
	h.r.record(fmt.Sprintf("note %s:%d", ec.SourceID, ec.SequenceNumber))
}

func pageCatalog(t *testing.T) *events.Catalog {
	t.Helper()
	input, err := events.Define[PageInput]()
	if err != nil {
		t.Fatal(err)
	}
	note, err := events.Define[PageNote]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(input.Descriptor(), note.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func pageReactor[R any](t *testing.T, factory func() R, options ...reactors.Option) *Reactor {
	t.Helper()
	declaration, err := reactors.Define[R](factory, options...)
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reactors.Compile(declaration, pageCatalog(t), models, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Reactor{plan: plan, store: "store", namespace: "Default"}
}

func appended(id, source string, number int) *contracts.AppendedEvent {
	return &contracts.AppendedEvent{
		Context: &contracts.EventContext{EventType: &contracts.EventType{Id: id, Generation: 1}, EventSourceId: source, EventStore: "store", Namespace: "Default", SequenceNumber: uint64(number), Occurred: &contracts.SerializableDateTimeOffset{Value: "2026-01-01T00:00:00Z"}},
		Content: fmt.Sprintf(`{"Number":%d}`, number),
	}
}

func partitionOf(source string, items ...*contracts.AppendedEvent) *contracts.PartitionEvents {
	return &contracts.PartitionEvents{Partition: source, Events: items}
}

func resultFor(t *testing.T, result *contracts.ReactorResult, partition string) *contracts.PartitionResult {
	t.Helper()
	for _, p := range result.Partitions {
		if p.Partition == partition {
			return p
		}
	}
	t.Fatalf("no result for partition %q in %v", partition, result)
	return nil
}

func TestLegacySinglePartitionMessageKeepsTheOldResultShape(t *testing.T) {
	recorder := &pageRecorder{failAt: 3}
	r := pageReactor(t, func() *PerEventReactor { return &PerEventReactor{recorder} })
	result := r.handle(t.Context(), &contracts.EventsToObserve{Partition: "a", DeliveryId: 9, Events: []*contracts.AppendedEvent{appended("PageInput", "a", 1), appended("PageInput", "a", 2), appended("PageInput", "a", 3), appended("PageInput", "a", 4)}})
	if result.Partition != "a" || result.DeliveryId != 9 || result.State != contracts.ObservationState_Failed || result.LastSuccessfulObservation != 2 || len(result.Partitions) != 0 {
		t.Fatalf("result = %v", result)
	}
	if !slices.Equal(recorder.calls, []string{"a:1", "a:2", "a:3"}) {
		t.Fatalf("calls = %v", recorder.calls)
	}
}

func TestPageSettlesEachPartitionOnItsOwn(t *testing.T) {
	recorder := &pageRecorder{failAt: 4}
	r := pageReactor(t, func() *PerEventReactor { return &PerEventReactor{recorder} })
	result := r.handle(t.Context(), &contracts.EventsToObserve{DeliveryId: 3, Origin: contracts.ObservationOrigin_Live, Partitions: []*contracts.PartitionEvents{
		partitionOf("a", appended("PageInput", "a", 1), appended("PageInput", "a", 3)),
		partitionOf("b", appended("PageInput", "b", 2), appended("PageInput", "b", 4), appended("PageInput", "b", 5)),
	}})
	if result.DeliveryId != 3 || len(result.Partitions) != 2 || result.Partition != "" {
		t.Fatalf("result = %v", result)
	}
	if a := resultFor(t, result, "a"); a.State != contracts.ObservationState_Success || a.LastSuccessfulObservation != 3 {
		t.Fatalf("a = %v", a)
	}
	if b := resultFor(t, result, "b"); b.State != contracts.ObservationState_Failed || b.LastSuccessfulObservation != 2 || len(b.ExceptionMessages) == 0 {
		t.Fatalf("b = %v", b)
	}
	if !slices.Equal(recorder.calls, []string{"a:1", "a:3", "b:2", "b:4"}) {
		t.Fatalf("per-event calls stop at a partition's failure only: %v", recorder.calls)
	}
}

var observesInput = reactors.WithBatchMethod("Index", observation.Observes[PageInput]())

func TestBatchAndPerEventMethodsEachSeeTheirOwnEventsInOrder(t *testing.T) {
	recorder := &pageRecorder{}
	r := pageReactor(t, func() *MixedReactor { return &MixedReactor{recorder} }, observesInput)
	result := r.handle(t.Context(), &contracts.EventsToObserve{DeliveryId: 1, Origin: contracts.ObservationOrigin_Replaying, DeliverWithinMilliseconds: 60_000, Partitions: []*contracts.PartitionEvents{
		partitionOf("a", appended("PageInput", "a", 1), appended("PageNote", "a", 4)),
		partitionOf("b", appended("PageInput", "b", 2), appended("PageInput", "b", 3), appended("PageNote", "b", 5)),
	}})
	for _, p := range result.Partitions {
		if p.State != contracts.ObservationState_Success {
			t.Fatalf("partition = %v", p)
		}
	}
	if len(recorder.batches) != 1 || !slices.Equal(recorder.batches[0], []events.SequenceNumber{1, 2, 3}) {
		t.Fatalf("one batch call across partitions in sequence order: %v", recorder.batches)
	}
	if !slices.Equal(recorder.calls, []string{"note a:4", "note b:5"}) {
		t.Fatalf("per-event calls = %v", recorder.calls)
	}
	bc := recorder.context[0]
	if bc.Origin != observation.OriginReplay || !bc.IsReplay() || !bc.CanDefer || bc.EventCount != 5 || bc.FirstSequenceNumber != 1 || bc.LastSequenceNumber != 5 || !slices.Equal(bc.Partitions, []events.SourceID{"a", "b"}) || bc.Deadline.IsZero() || bc.Observer != string(r.plan.Identifier()) || bc.Store != "store" {
		t.Fatalf("batch context = %+v", bc)
	}
}

func TestBatchFailAndDeferSettleAtTheEarliestProblem(t *testing.T) {
	recorder := &pageRecorder{batch: func(batch *observation.EventBatch) error {
		_ = batch.DeferEvent(events.Context{SourceID: "a", SequenceNumber: 3}, 2*time.Second)
		return batch.Fail("b", errors.New("bulk write rejected"))
	}}
	r := pageReactor(t, func() *MixedReactor { return &MixedReactor{recorder} }, observesInput)
	result := r.handle(t.Context(), &contracts.EventsToObserve{DeliveryId: 1, Partitions: []*contracts.PartitionEvents{
		partitionOf("a", appended("PageInput", "a", 1), appended("PageNote", "a", 2), appended("PageInput", "a", 3), appended("PageNote", "a", 6)),
		partitionOf("b", appended("PageInput", "b", 4), appended("PageNote", "b", 5)),
		partitionOf("c", appended("PageNote", "c", 7)),
	}})
	a := resultFor(t, result, "a")
	if a.State != contracts.ObservationState_Deferred || a.LastSuccessfulObservation != 2 || a.RetryAfterMilliseconds != 2000 {
		t.Fatalf("a = %v", a)
	}
	b := resultFor(t, result, "b")
	if b.State != contracts.ObservationState_Failed || b.LastSuccessfulObservation != uint64(events.Unavailable) || !strings.Contains(strings.Join(b.ExceptionMessages, ""), "bulk write rejected") {
		t.Fatalf("b = %v", b)
	}
	if c := resultFor(t, result, "c"); c.State != contracts.ObservationState_Success || c.LastSuccessfulObservation != 7 {
		t.Fatalf("c = %v", c)
	}
	if !slices.Equal(recorder.calls, []string{"note a:2", "note a:6", "note b:5", "note c:7"}) {
		t.Fatalf("order is kept per method, so per-event methods still run: %v", recorder.calls)
	}
}

func TestBatchMethodErrorFailsEachPartitionAtItsFirstBatchedEvent(t *testing.T) {
	recorder := &pageRecorder{batch: func(*observation.EventBatch) error { return errors.New("index down") }}
	r := pageReactor(t, func() *MixedReactor { return &MixedReactor{recorder} }, observesInput)
	result := r.handle(t.Context(), &contracts.EventsToObserve{Partitions: []*contracts.PartitionEvents{
		partitionOf("a", appended("PageNote", "a", 1), appended("PageInput", "a", 2)),
		partitionOf("b", appended("PageInput", "b", 3)),
	}})
	if a := resultFor(t, result, "a"); a.State != contracts.ObservationState_Failed || a.LastSuccessfulObservation != 1 {
		t.Fatalf("a = %v", a)
	}
	if b := resultFor(t, result, "b"); b.State != contracts.ObservationState_Failed || b.LastSuccessfulObservation != uint64(events.Unavailable) {
		t.Fatalf("b = %v", b)
	}
}

func TestDeferralOnALegacyMessageFailsThePartition(t *testing.T) {
	recorder := &pageRecorder{batch: func(batch *observation.EventBatch) error { return batch.Defer("a", time.Second) }}
	r := pageReactor(t, func() *MixedReactor { return &MixedReactor{recorder} }, observesInput)
	result := r.handle(t.Context(), &contracts.EventsToObserve{Partition: "a", Events: []*contracts.AppendedEvent{appended("PageInput", "a", 1)}})
	if result.State != contracts.ObservationState_Failed || !strings.Contains(strings.Join(result.ExceptionMessages, ""), "does not accept deferred partitions") {
		t.Fatalf("result = %v", result)
	}
	if recorder.context[0].CanDefer {
		t.Fatal("a legacy message must not offer deferral")
	}
}

func TestReactorRegistrationOptsIntoPageDelivery(t *testing.T) {
	r := pageReactor(t, func() *PerEventReactor { return &PerEventReactor{&pageRecorder{}} })
	stream := &capturingStream{}
	if _, err := Open(t.Context(), &capturingConn{stream: stream}, "connection", "store", "Default", r.plan, nil); err != nil {
		t.Fatal(err)
	}
	registration := stream.sent.(*contracts.ReactorMessage).GetContent().GetValue0()
	if !registration.GetPageDelivery().GetEnabled() {
		t.Fatalf("registration = %v", registration)
	}
}

type Balance struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

type BalanceReducer struct {
	calls  *atomic.Int32
	active *atomic.Int32
	peak   *atomic.Int32
}

func (r *BalanceReducer) Reduce(batch *observation.EventBatch, current *Balance) (*Balance, error) {
	r.calls.Add(1)
	now := r.active.Add(1)
	defer r.active.Add(-1)
	for peak := r.peak.Load(); now > peak && !r.peak.CompareAndSwap(peak, now); peak = r.peak.Load() {
	}
	time.Sleep(10 * time.Millisecond) // Overlap partitions; the assertion is on the bound, not on timing.
	balance := Balance{}
	if current != nil {
		balance = *current
	}
	for _, item := range batch.All() {
		balance.ID = string(item.Context.SourceID)
		balance.Amount += item.Event.(PageInput).Number
		if item.Event.(PageInput).Number == 0 {
			if err := batch.Defer(item.Context.SourceID, time.Second); err != nil {
				return nil, err
			}
		}
	}
	return &balance, nil
}

func reducerAppended(source string, number int) *reducercontracts.AppendedEvent {
	return &reducercontracts.AppendedEvent{
		Context: &reducercontracts.EventContext{EventType: &reducercontracts.EventType{Id: "PageInput", Generation: 1}, EventSourceId: source, EventStore: "store", Namespace: "Default", SequenceNumber: uint64(number), Occurred: &reducercontracts.SerializableDateTimeOffset{Value: "2026-01-01T00:00:00Z"}},
		Content: fmt.Sprintf(`{"Number":%d}`, number),
	}
}

func TestReducerFoldsEachPartitionOnceWithBoundedConcurrency(t *testing.T) {
	var calls, active, peak atomic.Int32
	model, err := readmodels.Define[Balance]()
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := reducers.Define[*BalanceReducer](model, func() *BalanceReducer { return &BalanceReducer{&calls, &active, &peak} }, reducers.WithBatchMethod("Reduce", observation.Observes[PageInput]()), reducers.WithMaxConcurrency(2))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reducers.Compile(declaration, pageCatalog(t), models, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := &Reducer{plan: plan, store: "store", namespace: "Default"}
	var changed []string
	r.OnChange = func(partition string, _ json.RawMessage) { changed = append(changed, partition) }
	operation := &reducercontracts.ReduceOperationMessage{DeliveryId: 5}
	for i, source := range []string{"a", "b", "c", "d"} {
		operation.Partitions = append(operation.Partitions, &reducercontracts.ReducerPartition{Partition: source, InitialState: `{"id":"x","amount":100}`, Events: []*reducercontracts.AppendedEvent{reducerAppended(source, i*2+1), reducerAppended(source, i*2+2)}})
	}
	operation.Partitions = append(operation.Partitions, &reducercontracts.ReducerPartition{Partition: "e", Events: []*reducercontracts.AppendedEvent{reducerAppended("e", 0)}})
	result := r.handle(t.Context(), operation)
	if calls.Load() != 5 || peak.Load() > 2 {
		t.Fatalf("calls = %d, peak = %d", calls.Load(), peak.Load())
	}
	if result.DeliveryId != 5 || len(result.Partitions) != 5 {
		t.Fatalf("result = %v", result)
	}
	for i, p := range result.Partitions[:4] {
		var state Balance
		if err := json.Unmarshal([]byte(p.ReadModelState), &state); err != nil {
			t.Fatal(err)
		}
		if p.State != reducercontracts.ObservationState_Success || p.LastSuccessfulObservation != uint64(i*2+2) || state.Amount != 100+i*4+3 {
			t.Fatalf("partition %d = %v", i, p)
		}
	}
	if e := result.Partitions[4]; e.State != reducercontracts.ObservationState_Deferred || e.RetryAfterMilliseconds != 1000 || e.ReadModelState != "" {
		t.Fatalf("deferred = %v", e)
	}
	if !slices.Equal(changed, []string{"a", "b", "c", "d"}) {
		t.Fatalf("changes = %v", changed)
	}
	legacy := r.handle(t.Context(), &reducercontracts.ReduceOperationMessage{Partition: "f", DeliveryId: 6, Events: []*reducercontracts.AppendedEvent{reducerAppended("f", 0)}})
	if legacy.Partition != "f" || legacy.State != reducercontracts.ObservationState_Failed || len(legacy.Partitions) != 0 || !strings.Contains(strings.Join(legacy.ExceptionMessages, ""), "does not accept deferred partitions") {
		t.Fatalf("legacy = %v", legacy)
	}
}

type capturingStream struct {
	grpc.ClientStream
	sent any
}

func (s *capturingStream) SendMsg(message any) error { s.sent = message; return nil }

type capturingConn struct {
	grpc.ClientConnInterface
	stream *capturingStream
}

func (c *capturingConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.stream, nil
}

func TestReducerRegistrationOptsIntoPageDelivery(t *testing.T) {
	model, err := readmodels.Define[Balance]()
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := reducers.DefineHandlers(model, "balances", []reducers.Handler{reducers.On(func(_ context.Context, _ PageInput, current *Balance, _ events.Context) (*Balance, error) {
		return current, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reducers.Compile(declaration, pageCatalog(t), models, nil)
	if err != nil {
		t.Fatal(err)
	}
	stream := &capturingStream{}
	if _, err := OpenReducer(t.Context(), &capturingConn{stream: stream}, "connection", "store", "Default", plan); err != nil {
		t.Fatal(err)
	}
	if !stream.sent.(*reducercontracts.ReducerMessage).GetContent().GetValue0().GetPageDelivery().GetEnabled() {
		t.Fatal("reducer registration does not opt into page delivery")
	}
}
