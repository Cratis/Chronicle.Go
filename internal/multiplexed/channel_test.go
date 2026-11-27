// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package multiplexed

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation/multiplexed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeCall is the kernel side of one shared call.
type fakeCall struct {
	grpc.ClientStream
	ctx     context.Context
	writes  chan *contracts.MultiplexedClientMessage
	reads   chan any // *MultiplexedServerMessage or error
	sendErr error
	gate    chan struct{} // When set, Send waits for it to close.
}

func (f *fakeCall) Send(message *contracts.MultiplexedClientMessage) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-f.ctx.Done():
			return io.EOF
		}
	}
	select {
	case f.writes <- message:
		return nil
	case <-f.ctx.Done():
		return io.EOF
	}
}

func (f *fakeCall) Recv() (*contracts.MultiplexedServerMessage, error) {
	select {
	case item := <-f.reads:
		if err, ok := item.(error); ok {
			return nil, err
		}
		return item.(*contracts.MultiplexedServerMessage), nil
	case <-f.ctx.Done():
		return nil, status.FromContextError(f.ctx.Err()).Err()
	}
}

type kernel struct {
	mu    sync.Mutex
	calls []*fakeCall
	opens atomic.Int32
	fail  error
}

func (k *kernel) open(ctx context.Context) (Call, error) {
	k.opens.Add(1)
	if k.fail != nil {
		return nil, k.fail
	}
	call := &fakeCall{ctx: ctx, writes: make(chan *contracts.MultiplexedClientMessage, 64), reads: make(chan any, 64)}
	k.mu.Lock()
	k.calls = append(k.calls, call)
	k.mu.Unlock()
	return call, nil
}

func (k *kernel) call(i int) *fakeCall {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.calls[i]
}

func newChannel(t *testing.T, coalesce time.Duration) (*Channel, *kernel, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	k := &kernel{}
	return NewChannel(ctx, k.open, coalesce), k, ctx
}

func register(kind contracts.MultiplexedObserverKind) *contracts.MultiplexedStreamMessage {
	if kind == contracts.MultiplexedObserverKind_Reducer {
		return &contracts.MultiplexedStreamMessage{Reducer: &contracts.ReducerMessage{}}
	}
	return &contracts.MultiplexedStreamMessage{Reactor: &contracts.ReactorMessage{}}
}

func write(t *testing.T, ctx context.Context, call *fakeCall) *contracts.MultiplexedClientMessage {
	t.Helper()
	select {
	case message := <-call.writes:
		return message
	case <-ctx.Done():
		t.Fatal("nothing was written")
		return nil
	}
}

func reactorMessage(id, sequence uint64) *contracts.MultiplexedServerMessage {
	return &contracts.MultiplexedServerMessage{StreamId: id, Sequence: sequence, Reactor: &contracts.EventsToObserve{Partition: "p"}}
}

func receive(t *testing.T, s *Stream) (*contracts.MultiplexedServerMessage, error) {
	t.Helper()
	type result struct {
		message *contracts.MultiplexedServerMessage
		err     error
	}
	done := make(chan result, 1)
	go func() {
		message, err := s.Receive()
		done <- result{message, err}
	}()
	select {
	case r := <-done:
		return r.message, r.err
	case <-time.After(3 * time.Second):
		t.Fatal("receive timed out")
		return nil, nil
	}
}

func TestRegistrationsReadyTogetherAreWrittenTogether(t *testing.T) {
	channel, k, ctx := newChannel(t, 50*time.Millisecond)
	var streams []*Stream
	for _, kind := range []contracts.MultiplexedObserverKind{contracts.MultiplexedObserverKind_Reactor, contracts.MultiplexedObserverKind_Reducer, contracts.MultiplexedObserverKind_Reactor} {
		stream, err := channel.Open(ctx, kind)
		if err != nil {
			t.Fatal(err)
		}
		if err = stream.Send(register(kind)); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, stream)
	}
	written := write(t, ctx, k.call(0))
	if len(written.Messages) != 3 || k.opens.Load() != 1 {
		t.Fatalf("expected one write of three registrations on one call, got %d messages, %d calls", len(written.Messages), k.opens.Load())
	}
	ids := map[uint64]bool{}
	for i, message := range written.Messages {
		ids[message.StreamId] = true
		if message.Kind != contracts.MultiplexedObserverKind(map[int]int{0: 1, 1: 2, 2: 1}[i]) {
			t.Fatalf("message %d has kind %v", i, message.Kind)
		}
	}
	if len(ids) != 3 || ids[0] {
		t.Fatalf("stream ids are not unique and non-zero: %v", ids)
	}
	// Later messages of a stream do not repeat the kind.
	if err := streams[0].Send(register(contracts.MultiplexedObserverKind_Reactor)); err != nil {
		t.Fatal(err)
	}
	if next := write(t, ctx, k.call(0)); next.Messages[0].Kind != contracts.MultiplexedObserverKind_Unspecified || next.Messages[0].StreamId != written.Messages[0].StreamId {
		t.Fatal(next)
	}
}

func TestAWriteCarriesAtMostFiveHundredMessages(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	stream, err := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	k.call(0).gate = gate
	for range MaxMessagesPerWrite*2 + 10 {
		if err = stream.Send(register(contracts.MultiplexedObserverKind_Reactor)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(50 * time.Millisecond) // let the writer take its first message and block
	close(gate)
	total, largest := 0, 0
	for total < MaxMessagesPerWrite+10 {
		message := write(t, ctx, k.call(0))
		total += len(message.Messages)
		largest = max(largest, len(message.Messages))
	}
	if largest != MaxMessagesPerWrite {
		t.Fatalf("the largest write carried %d messages, expected exactly the cap", largest)
	}
}

func TestMessagesOfInterleavedStreamsAreDeliveredInOrderToTheirOwnStream(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	first, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	second, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reducer)
	call := k.call(0)
	call.reads <- reactorMessage(first.id, 1)
	call.reads <- &contracts.MultiplexedServerMessage{StreamId: second.id, Sequence: 1, Reducer: &contracts.ReduceOperationMessage{Partition: "r1"}}
	call.reads <- reactorMessage(first.id, 2)
	call.reads <- &contracts.MultiplexedServerMessage{StreamId: second.id, Sequence: 2, Reducer: &contracts.ReduceOperationMessage{Partition: "r2"}}
	for sequence := uint64(1); sequence <= 2; sequence++ {
		if message, err := receive(t, first); err != nil || message.Sequence != sequence || message.Reactor == nil {
			t.Fatal(message, err)
		}
		if message, err := receive(t, second); err != nil || message.Sequence != sequence || message.Reducer == nil {
			t.Fatal(message, err)
		}
	}
}

func TestAStreamEndingWithAStatusFailsOnlyThatStream(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	failing, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	healthy, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	call := k.call(0)
	call.reads <- &contracts.MultiplexedServerMessage{StreamId: failing.id, Sequence: 1, End: &contracts.MultiplexedStreamEnd{StatusCode: int32(codes.FailedPrecondition), Detail: "client is not connected"}}
	call.reads <- reactorMessage(healthy.id, 1)
	_, err := receive(t, failing)
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "client is not connected" {
		t.Fatalf("got %v", err)
	}
	if message, err := receive(t, healthy); err != nil || message.Sequence != 1 {
		t.Fatal(message, err)
	}
}

func TestAStreamEndingWithStatusZeroCompletes(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	stream, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	other, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	k.call(0).reads <- &contracts.MultiplexedServerMessage{StreamId: stream.id, Sequence: 1, End: &contracts.MultiplexedStreamEnd{}}
	if _, err := receive(t, stream); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v", err)
	}
	k.call(0).reads <- reactorMessage(other.id, 1)
	if _, err := receive(t, other); err != nil {
		t.Fatal(err)
	}
}

func TestClosingAStreamTellsTheKernelAndKeepsTheCallForTheOthers(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	closing, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	other, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	if err := closing.Send(register(contracts.MultiplexedObserverKind_Reactor)); err != nil {
		t.Fatal(err)
	}
	write(t, ctx, k.call(0))
	closing.Close()
	message := write(t, ctx, k.call(0))
	if len(message.Messages) != 1 || !message.Messages[0].Close || message.Messages[0].StreamId != closing.id || message.Messages[0].Reactor != nil {
		t.Fatal(message)
	}
	if err := closing.Send(register(contracts.MultiplexedObserverKind_Reactor)); !errors.Is(err, io.EOF) {
		t.Fatalf("send after close: %v", err)
	}
	if k.call(0).ctx.Err() != nil {
		t.Fatal("the shared call ended while a stream was open")
	}
	k.call(0).reads <- reactorMessage(other.id, 1)
	if _, err := receive(t, other); err != nil {
		t.Fatal(err)
	}
}

func TestTheLastStreamClosingEndsTheCallAndTheNextStreamOpensANewOne(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	stream, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	stream.Close()
	select {
	case <-k.call(0).ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the shared call was not cancelled")
	}
	if _, err := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor); err != nil || k.opens.Load() != 2 {
		t.Fatalf("err %v opens %d", err, k.opens.Load())
	}
}

func TestAStreamContextEndingClosesTheStream(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	observerCtx, cancel := context.WithCancel(ctx)
	stream, _ := channel.Open(observerCtx, contracts.MultiplexedObserverKind_Reactor)
	other, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	if err := stream.Send(register(contracts.MultiplexedObserverKind_Reactor)); err != nil {
		t.Fatal(err)
	}
	write(t, ctx, k.call(0))
	cancel()
	if _, err := receive(t, stream); status.Code(err) != codes.Canceled {
		t.Fatalf("got %v", err)
	}
	if message := write(t, ctx, k.call(0)); !message.Messages[0].Close {
		t.Fatal(message)
	}
	_ = other
}

func TestAFailureOfTheSharedCallFailsEveryStreamAndTheNextStreamUsesANewCall(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	one, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	two, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reducer)
	k.call(0).reads <- status.Error(codes.Unavailable, "kernel went away")
	for _, stream := range []*Stream{one, two} {
		if _, err := receive(t, stream); status.Code(err) != codes.Unavailable {
			t.Fatalf("got %v", err)
		}
	}
	if err := one.Send(register(contracts.MultiplexedObserverKind_Reactor)); !errors.Is(err, io.EOF) {
		t.Fatalf("send on a failed call: %v", err)
	}
	replacement, err := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	if err != nil || k.opens.Load() != 2 {
		t.Fatalf("err %v opens %d", err, k.opens.Load())
	}
	k.call(1).reads <- reactorMessage(replacement.id, 1)
	if _, err = receive(t, replacement); err != nil {
		t.Fatal(err)
	}
}

func TestTheSharedCallEndingCompletesEveryStream(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	one, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	k.call(0).reads <- io.EOF
	if _, err := receive(t, one); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v", err)
	}
}

func TestASequenceGapFailsTheWholeCall(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	one, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	two, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	call := k.call(0)
	call.reads <- reactorMessage(one.id, 1)
	call.reads <- reactorMessage(two.id, 2) // never saw 1
	if _, err := receive(t, one); err != nil {
		t.Fatalf("the message before the gap is still delivered: %v", err)
	}
	for _, stream := range []*Stream{one, two} {
		if _, err := receive(t, stream); status.Code(err) != codes.Internal {
			t.Fatalf("got %v", err)
		}
	}
	if call.ctx.Err() == nil {
		t.Fatal("the shared call was kept")
	}
}

func TestASlowObserverDoesNotBlockTheOthers(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	slow, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	fast, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	call := k.call(0)
	const count = 200 // far beyond the fake call's own buffering
	go func() {
		for i := uint64(1); i <= count; i++ {
			call.reads <- reactorMessage(slow.id, i)
			call.reads <- reactorMessage(fast.id, i)
		}
	}()
	for i := uint64(1); i <= count; i++ {
		if message, err := receive(t, fast); err != nil || message.Sequence != i {
			t.Fatal(message, err)
		}
	}
	// The slow stream's messages were retained, in order, while nobody read them.
	for i := uint64(1); i <= count; i++ {
		if message, err := receive(t, slow); err != nil || message.Sequence != i {
			t.Fatal(message, err)
		}
	}
}

func TestAMessageForAStreamAlreadyClosedIsIgnored(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	closed, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	open, _ := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor)
	closed.Close()
	k.call(0).reads <- reactorMessage(closed.id, 1)
	k.call(0).reads <- reactorMessage(open.id, 1)
	if message, err := receive(t, open); err != nil || message.Sequence != 1 {
		t.Fatal(message, err)
	}
}

func TestOpeningTheSharedCallFailingIsReportedToTheStream(t *testing.T) {
	channel, k, ctx := newChannel(t, 0)
	k.fail = status.Error(codes.Unimplemented, "no multiplexing")
	if _, err := channel.Open(ctx, contracts.MultiplexedObserverKind_Reactor); status.Code(err) != codes.Unimplemented {
		t.Fatalf("got %v", err)
	}
}
