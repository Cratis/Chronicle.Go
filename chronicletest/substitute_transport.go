// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// substituteTransport dispatches the generated service descriptors directly. It
// does not host a server, serialize HTTP/2, or provide network/security fidelity.
// Registration finishes before use. Each admitted call is canceled and joined by
// Close; unary handlers run synchronously and streams own one joined goroutine.
type substituteTransport struct {
	unary   map[string]substituteUnary
	streams map[string]substituteStreaming
	mu      sync.Mutex
	closed  bool
	active  map[*substituteCall]struct{}
	work    sync.WaitGroup
}

type substituteUnary struct {
	service any
	method  grpc.MethodDesc
}

type substituteStreaming struct {
	service any
	method  grpc.StreamDesc
}

type substituteCall struct {
	ctx    context.Context
	cancel context.CancelFunc
}

var (
	_ grpc.ClientConnInterface = (*substituteTransport)(nil)
	_ grpc.ServiceRegistrar    = (*substituteTransport)(nil)
)

func newSubstituteTransport() *substituteTransport {
	return &substituteTransport{
		unary:   make(map[string]substituteUnary),
		streams: make(map[string]substituteStreaming),
		active:  make(map[*substituteCall]struct{}),
	}
}

func (t *substituteTransport) RegisterService(desc *grpc.ServiceDesc, service any) {
	for _, method := range desc.Methods {
		t.unary["/"+desc.ServiceName+"/"+method.MethodName] = substituteUnary{service, method}
	}
	for _, method := range desc.Streams {
		t.streams["/"+desc.ServiceName+"/"+method.StreamName] = substituteStreaming{service, method}
	}
}

func (t *substituteTransport) begin(ctx context.Context) (*substituteCall, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, substituteStatus(err)
	}
	if t.closed {
		return nil, status.Error(codes.Unavailable, "substitute transport is closed")
	}
	ctx, cancel := context.WithCancel(ctx)
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		ctx = metadata.NewIncomingContext(ctx, md.Copy())
	}
	call := &substituteCall{ctx: ctx, cancel: cancel}
	t.active[call] = struct{}{}
	t.work.Add(1)
	return call, nil
}

func (t *substituteTransport) finish(call *substituteCall) {
	call.cancel()
	t.mu.Lock()
	delete(t.active, call)
	t.mu.Unlock()
	t.work.Done()
}

func (t *substituteTransport) Close() error {
	t.mu.Lock()
	t.closed = true
	for call := range t.active {
		call.cancel()
	}
	t.mu.Unlock()
	t.work.Wait()
	return nil
}

func (t *substituteTransport) Invoke(ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	call, err := t.begin(ctx)
	if err != nil {
		return err
	}
	defer t.finish(call)
	handler, ok := t.unary[method]
	if !ok {
		return unsupported("RPC " + method)
	}
	response, err := handler.method.Handler(handler.service, call.ctx, func(request any) error {
		return copySubstituteMessage(request, args)
	}, nil)
	if canceled := call.ctx.Err(); canceled != nil {
		return substituteStatus(canceled)
	}
	if err != nil {
		return substituteStatus(err)
	}
	return copySubstituteMessage(reply, response)
}

func (t *substituteTransport) NewStream(ctx context.Context, _ *grpc.StreamDesc, method string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	call, err := t.begin(ctx)
	if err != nil {
		return nil, err
	}
	handler, ok := t.streams[method]
	if !ok {
		t.finish(call)
		return nil, unsupported("RPC " + method)
	}
	stream := newSubstituteStream(call)
	go func() {
		defer t.finish(call)
		err := handler.method.Handler(handler.service, &substituteServerStream{stream})
		if canceled := call.ctx.Err(); canceled != nil {
			err = canceled
		}
		stream.err = substituteStatus(err)
		stream.publishHeader(stream.err)
		// Only the handler sends responses. Publish the terminal status before
		// closing, so queued messages are received in order before status/EOF.
		close(stream.responses)
		close(stream.done)
	}()
	return stream, nil
}

func substituteStatus(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return status.Convert(err).Err()
}

func cloneSubstituteMessage(value any) (proto.Message, error) {
	message, ok := value.(proto.Message)
	if !ok || !message.ProtoReflect().IsValid() {
		return nil, status.Error(codes.Internal, "substitute transport requires a non-nil protobuf message")
	}
	return proto.Clone(message), nil
}

func copySubstituteMessage(destination, source any) error {
	snapshot, err := cloneSubstituteMessage(source)
	if err != nil {
		return err
	}
	message, ok := destination.(proto.Message)
	if !ok || !message.ProtoReflect().IsValid() || message.ProtoReflect().Descriptor() != snapshot.ProtoReflect().Descriptor() {
		return status.Error(codes.Internal, "substitute transport received an incompatible protobuf message")
	}
	proto.Reset(message)
	proto.Merge(message, snapshot)
	return nil
}
