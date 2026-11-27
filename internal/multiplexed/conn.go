// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package multiplexed

import (
	"context"
	"fmt"

	contracts "github.com/cratis/chronicle.go/contracts/observation/multiplexed"
	reactors "github.com/cratis/chronicle.go/contracts/observation/reactors"
	reducers "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Conn serves the per-observer Reactors/Observe and Reducers/Observe streams
// over the shared channel and passes every other RPC to the connection it wraps.
// The observers keep using their own generated clients and messages: the
// multiplexed messages are wire-identical to them, so a message is converted by
// its wire encoding.
type Conn struct {
	grpc.ClientConnInterface
	channel *Channel
}

// NewConn wraps conn so observer streams share channel.
func NewConn(conn grpc.ClientConnInterface, channel *Channel) *Conn {
	return &Conn{ClientConnInterface: conn, channel: channel}
}

// NewStream opens a multiplexed stream for an observation method and delegates others.
func (c *Conn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	switch method {
	case reactors.Reactors_Observe_FullMethodName:
		return c.open(ctx, contracts.MultiplexedObserverKind_Reactor)
	case reducers.Reducers_Observe_FullMethodName:
		return c.open(ctx, contracts.MultiplexedObserverKind_Reducer)
	default:
		return c.ClientConnInterface.NewStream(ctx, desc, method, options...)
	}
}

func (c *Conn) open(ctx context.Context, kind contracts.MultiplexedObserverKind) (grpc.ClientStream, error) {
	stream, err := c.channel.Open(ctx, kind)
	if err != nil {
		return nil, err
	}
	return &observerStream{stream: stream, kind: kind, ctx: ctx}, nil
}

// observerStream adapts a multiplexed Stream to the grpc.ClientStream the
// generated Reactors and Reducers clients wrap.
type observerStream struct {
	stream *Stream
	kind   contracts.MultiplexedObserverKind
	ctx    context.Context
}

func (s *observerStream) Header() (metadata.MD, error) { return metadata.MD{}, nil }
func (s *observerStream) Trailer() metadata.MD         { return metadata.MD{} }
func (s *observerStream) Context() context.Context     { return s.ctx }

// CloseSend ends the observer's side, which closes the stream on the kernel.
func (s *observerStream) CloseSend() error {
	s.stream.Close()
	return nil
}

func (s *observerStream) SendMsg(message any) error {
	wire, err := marshal(message)
	if err != nil {
		return err
	}
	out := &contracts.MultiplexedStreamMessage{}
	switch s.kind {
	case contracts.MultiplexedObserverKind_Reactor:
		out.Reactor = &contracts.ReactorMessage{}
		err = proto.Unmarshal(wire, out.Reactor)
		preserveNotReplayable(out.Reactor)
	default:
		out.Reducer = &contracts.ReducerMessage{}
		err = proto.Unmarshal(wire, out.Reducer)
	}
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("multiplexed: %v", err))
	}
	return s.stream.Send(out)
}

func (s *observerStream) RecvMsg(message any) error {
	in, err := s.stream.Receive()
	if err != nil {
		return err
	}
	var content proto.Message
	switch {
	case in.Reactor != nil && s.kind == contracts.MultiplexedObserverKind_Reactor:
		content = in.Reactor
	case in.Reducer != nil && s.kind == contracts.MultiplexedObserverKind_Reducer:
		content = in.Reducer
	default:
		return status.Error(codes.Internal, "multiplexed: the stream message has no content for its observer")
	}
	wire, err := proto.Marshal(content)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("multiplexed: %v", err))
	}
	target, ok := message.(proto.Message)
	if !ok {
		return status.Error(codes.Internal, "multiplexed: the receive target is not a protobuf message")
	}
	if err = proto.Unmarshal(wire, target); err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("multiplexed: %v", err))
	}
	return nil
}

// preserveNotReplayable writes the explicit false of IsReplayable back. The
// kernel's serializer defaults an absent field 4 to true, proto3 omits false,
// and decoding the wire bytes into this package's type turns the explicit false
// the per-observer registration carries into the omitted default.
func preserveNotReplayable(message *contracts.ReactorMessage) {
	definition := message.GetContent().GetValue0().GetReactor()
	if definition != nil && !definition.IsReplayable {
		definition.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 4, protowire.VarintType), 0))
	}
}

func marshal(message any) ([]byte, error) {
	value, ok := message.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "multiplexed: the message is not a protobuf message")
	}
	wire, err := proto.Marshal(value)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("multiplexed: %v", err))
	}
	return wire, nil
}

// NewOpener opens the shared MultiplexedObservation call over conn.
func NewOpener(conn grpc.ClientConnInterface) Opener {
	return func(ctx context.Context) (Call, error) {
		return contracts.NewMultiplexedObservationClient(conn).Observe(ctx)
	}
}
