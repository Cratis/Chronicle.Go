// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package multiplexed

import (
	"context"
	"errors"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/multiplexed"
	reactors "github.com/cratis/chronicle.go/contracts/observation/reactors"
	reducers "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// innerConn records the RPCs that are not multiplexed.
type innerConn struct {
	invoked, streamed []string
}

func (i *innerConn) Invoke(_ context.Context, method string, _, _ any, _ ...grpc.CallOption) error {
	i.invoked = append(i.invoked, method)
	return nil
}

func (i *innerConn) NewStream(_ context.Context, _ *grpc.StreamDesc, method string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	i.streamed = append(i.streamed, method)
	return nil, errors.New("not multiplexed")
}

func newConn(t *testing.T) (*Conn, *kernel, *innerConn, context.Context) {
	t.Helper()
	channel, k, ctx := newChannel(t, 0)
	inner := &innerConn{}
	return NewConn(inner, channel), k, inner, ctx
}

func TestAReactorObservesThroughTheSharedCall(t *testing.T) {
	conn, k, inner, ctx := newConn(t)
	stream, err := reactors.NewReactorsClient(conn).Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	registration := &reactors.RegisterReactor{ConnectionId: "connection", EventStore: "store", Namespace: "ns", Reactor: &reactors.ReactorDefinition{ReactorId: "r", IsReplayable: false}, PageDelivery: &reactors.PageDelivery{Enabled: true}}
	// The per-observer registration carries an explicit false for field 4, as protobuf-net defaults it to true.
	registration.Reactor.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 4, protowire.VarintType), 0))
	if err = stream.Send(&reactors.ReactorMessage{Content: &reactors.OneOf_RegisterReactor_ReactorResult{Value0: registration}}); err != nil {
		t.Fatal(err)
	}
	written := write(t, ctx, k.call(0)).Messages[0]
	got := written.Reactor.Content.Value0
	if written.Kind != contracts.MultiplexedObserverKind_Reactor || got.ConnectionId != "connection" || got.Reactor.ReactorId != "r" || !got.PageDelivery.Enabled {
		t.Fatal(written)
	}
	definition, err := proto.Marshal(got.Reactor)
	if err != nil {
		t.Fatal(err)
	}
	if !containsExplicitFalse(definition, 4) {
		t.Fatalf("the explicit IsReplayable=false was lost: % x", definition)
	}
	if err = stream.Send(&reactors.ReactorMessage{Content: &reactors.OneOf_RegisterReactor_ReactorResult{Value1: &reactors.ReactorResult{DeliveryId: 7, State: reactors.ObservationState_Success}}}); err != nil {
		t.Fatal(err)
	}
	if later := write(t, ctx, k.call(0)).Messages[0]; later.Reactor.Content.Value1.DeliveryId != 7 || later.Kind != contracts.MultiplexedObserverKind_Unspecified {
		t.Fatal(later)
	}
	k.call(0).reads <- &contracts.MultiplexedServerMessage{StreamId: written.StreamId, Sequence: 1, Reactor: &contracts.EventsToObserve{Partition: "source", DeliveryId: 9, Events: []*contracts.AppendedEvent{{Content: "{}", Context: &contracts.EventContext{SequenceNumber: 3}}}}}
	batch, err := stream.Recv()
	if err != nil || batch.Partition != "source" || batch.DeliveryId != 9 || batch.Events[0].Context.SequenceNumber != 3 {
		t.Fatal(batch, err)
	}
	if len(inner.streamed) != 0 {
		t.Fatalf("observation reached the per-observer RPC: %v", inner.streamed)
	}
}

func containsExplicitFalse(message []byte, number protowire.Number) bool {
	for len(message) > 0 {
		field, wire, n := protowire.ConsumeTag(message)
		if n < 0 {
			return false
		}
		message = message[n:]
		size := protowire.ConsumeFieldValue(field, wire, message)
		if size < 0 {
			return false
		}
		if field == number && wire == protowire.VarintType {
			value, _ := protowire.ConsumeVarint(message)
			if value == 0 {
				return true
			}
		}
		message = message[size:]
	}
	return false
}

func TestAReducerObservesThroughTheSharedCall(t *testing.T) {
	conn, k, _, ctx := newConn(t)
	stream, err := reducers.NewReducersClient(conn).Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Send(&reducers.ReducerMessage{Content: &reducers.OneOf_RegisterReducer_ReducerResult{Value0: &reducers.RegisterReducer{ConnectionId: "c", Reducer: &reducers.ReducerDefinition{ReducerId: "fold"}, StateCaching: &reducers.StateCaching{Enabled: true}}}}); err != nil {
		t.Fatal(err)
	}
	written := write(t, ctx, k.call(0)).Messages[0]
	if written.Kind != contracts.MultiplexedObserverKind_Reducer || written.Reducer.Content.Value0.Reducer.ReducerId != "fold" || !written.Reducer.Content.Value0.StateCaching.Enabled {
		t.Fatal(written)
	}
	k.call(0).reads <- &contracts.MultiplexedServerMessage{StreamId: written.StreamId, Sequence: 1, Reducer: &contracts.ReduceOperationMessage{Partition: "p", InitialState: "{}", DeliveryId: 4}}
	operation, err := stream.Recv()
	if err != nil || operation.Partition != "p" || operation.DeliveryId != 4 {
		t.Fatal(operation, err)
	}
}

func TestEndingTheSendSideClosesTheStream(t *testing.T) {
	conn, k, _, ctx := newConn(t)
	stream, _ := reactors.NewReactorsClient(conn).Observe(ctx)
	// Another observer keeps the shared call alive; with none left the call itself is cancelled instead.
	if _, err := reducers.NewReducersClient(conn).Observe(ctx); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&reactors.ReactorMessage{Content: &reactors.OneOf_RegisterReactor_ReactorResult{Value0: &reactors.RegisterReactor{}}}); err != nil {
		t.Fatal(err)
	}
	write(t, ctx, k.call(0))
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if message := write(t, ctx, k.call(0)).Messages[0]; !message.Close {
		t.Fatal(message)
	}
}

func TestOtherRPCsAreNotMultiplexed(t *testing.T) {
	conn, k, inner, ctx := newConn(t)
	if err := conn.Invoke(ctx, reactors.Reactors_HasReactor_FullMethodName, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.NewStream(ctx, &grpc.StreamDesc{}, "/other/Stream"); err == nil {
		t.Fatal("expected the delegated stream's error")
	}
	if len(inner.invoked) != 1 || len(inner.streamed) != 1 || k.opens.Load() != 0 {
		t.Fatalf("invoked %v streamed %v shared calls %d", inner.invoked, inner.streamed, k.opens.Load())
	}
}

// The observers keep their own generated messages and the conversion is by wire
// encoding, so the fields must agree by number and type.
func TestMultiplexedObserverMessagesAreWireIdenticalToThePerObserverOnes(t *testing.T) {
	pairs := map[string][2]protoreflect.MessageDescriptor{
		"reactor": {(&reactors.ReactorMessage{}).ProtoReflect().Descriptor(), (&contracts.ReactorMessage{}).ProtoReflect().Descriptor()},
		"reducer": {(&reducers.ReducerMessage{}).ProtoReflect().Descriptor(), (&contracts.ReducerMessage{}).ProtoReflect().Descriptor()},
		"events":  {(&reactors.EventsToObserve{}).ProtoReflect().Descriptor(), (&contracts.EventsToObserve{}).ProtoReflect().Descriptor()},
		"reduce":  {(&reducers.ReduceOperationMessage{}).ProtoReflect().Descriptor(), (&contracts.ReduceOperationMessage{}).ProtoReflect().Descriptor()},
	}
	for name, pair := range pairs {
		compareDescriptors(t, name, pair[0], pair[1], map[protoreflect.FullName]bool{})
	}
}

func compareDescriptors(t *testing.T, path string, a, b protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) {
	t.Helper()
	if seen[a.FullName()] {
		return
	}
	seen[a.FullName()] = true
	if a.Fields().Len() != b.Fields().Len() {
		t.Errorf("%s: %s has %d fields, %s has %d", path, a.FullName(), a.Fields().Len(), b.FullName(), b.Fields().Len())
		return
	}
	for i := range a.Fields().Len() {
		fa, fb := a.Fields().Get(i), b.Fields().Get(i)
		if fa.Number() != fb.Number() || fa.Kind() != fb.Kind() || fa.Cardinality() != fb.Cardinality() || fa.Name() != fb.Name() {
			t.Errorf("%s.%s: field %d differs (%s/%v vs %s/%v)", path, a.Name(), fa.Number(), fa.Name(), fa.Kind(), fb.Name(), fb.Kind())
			continue
		}
		switch fa.Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			if fa.IsMap() {
				if fa.MapValue().Kind() != protoreflect.MessageKind {
					continue
				}
				compareDescriptors(t, path+"."+string(fa.Name()), fa.MapValue().Message(), fb.MapValue().Message(), seen)
			} else {
				compareDescriptors(t, path+"."+string(fa.Name()), fa.Message(), fb.Message(), seen)
			}
		case protoreflect.EnumKind:
			if fa.Enum().Values().Len() != fb.Enum().Values().Len() {
				t.Errorf("%s.%s: enum values differ", path, fa.Name())
			}
		}
	}
}
