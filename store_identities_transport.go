// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"reflect"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type identityFailure struct {
	reason          string
	category        error // Only fixed SDK identities or direct context sentinels.
	unauthenticated bool
	refused         bool  // Decoder-backed explicit command refusal only.
	cancellation    error // Direct context sentinel only.
}

func (identityFailure) Error() string { return "chronicle: identity operation boundary failed" }

func identityContextFailure(err error) identityFailure {
	if err == context.Canceled || err == context.DeadlineExceeded {
		return identityFailure{reason: "cancellation", category: err}
	}
	return identityFailure{reason: "cancellation"}
}

// Do not use status.Code/FromError or errors.Is/As here. Exact grpc-go ownership
// is established without invoking a borrowed error hook; wrappers stay opaque.
var identityStatusErrorType = reflect.TypeOf(status.Error(codes.Unknown, ""))

func identitySanitize(err error) identityFailure {
	if err == nil {
		return identityFailure{}
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		return identityContextFailure(err)
	}
	for _, category := range []error{ErrProtocol, ErrUnsupported, ErrClosed, ErrInvalidConfiguration} {
		if err == category {
			return identityFailure{reason: "transport", category: category}
		}
	}
	if reflect.TypeOf(err) == reflect.TypeFor[*AuthenticationError]() {
		return identityFailure{reason: "authorization", category: &AuthenticationError{}}
	}
	if reflect.TypeOf(err) != identityStatusErrorType || nilValue(err) {
		return identityFailure{reason: "transport"}
	}
	code := err.(interface{ GRPCStatus() *status.Status }).GRPCStatus().Code()
	switch code {
	case codes.Canceled:
		return identityContextFailure(context.Canceled)
	case codes.DeadlineExceeded:
		return identityContextFailure(context.DeadlineExceeded)
	case codes.Unauthenticated:
		return identityFailure{reason: "authorization", category: &AuthenticationError{}, unauthenticated: true}
	case codes.PermissionDenied:
		return identityFailure{reason: "authorization"}
	case codes.Unimplemented:
		return identityFailure{reason: "transport", category: ErrUnsupported}
	default:
		return identityFailure{reason: "transport"}
	}
}

type identityTokens struct{ source TokenSource }

func (s identityTokens) Token(ctx context.Context) (token Token, err error) {
	// This recovery wraps only the borrowed callback, never SDK processing.
	borrowed, panicked := identityBorrowToken(ctx, s.source)
	if panicked {
		return Token{}, identityFailure{reason: "authorization"}
	}
	if borrowed.err != nil {
		return Token{}, identitySanitize(borrowed.err)
	}
	return borrowed.token, nil
}

type identityTokenReply struct {
	token Token
	err   error
}

func identityBorrowToken(ctx context.Context, source TokenSource) (reply identityTokenReply, panicked bool) {
	panicked = true
	defer func() { _ = recover() }()
	reply.token, reply.err = source.Token(ctx)
	panicked = false
	return
}
func identityInvalidate(source TokenSource) {
	if invalidator, ok := source.(TokenInvalidator); ok {
		func() { defer func() { _ = recover() }(); invalidator.Invalidate() }()
	}
}

// The operation never calls a shared acquire/registration/transport producer.
type identityInvocation struct {
	store      *EventStore
	generation *generation
	root       *definitionRoot
	readiness  identityReadiness
	ctx        context.Context
}

func (i *identityInvocation) check() identityFailure {
	i.store.client.mu.Lock()
	defer i.store.client.mu.Unlock()
	return i.store.identityReadyLocked(i.ctx, i.generation, i.root, i.readiness, false)
}

func (i *identityInvocation) invoke(method string, request, response any, command bool, classify func(*identityCodec) identityFailure) (entered bool, failure identityFailure) {
	if failure = i.check(); failure.reason != "" {
		return
	}
	var source TokenSource
	if i.generation.tokens != nil {
		source = identityTokens{i.generation.tokens}
	}
	ctx, err := authorize(i.ctx, source)
	if err != nil {
		if safe, ok := err.(identityFailure); ok {
			failure = safe
		} else {
			failure = identitySanitize(err)
		}
		return
	}
	c, g, d := i.store.client, i.generation, i.store.definitions
	c.mu.Lock()
	failure = i.store.identityReadyLocked(ctx, g, i.root, i.readiness, false)
	if failure.reason != "" {
		c.mu.Unlock()
		return
	}
	if command {
		d.flight = true
	}
	g.work.Add(1)
	c.work.Add(1)
	c.mu.Unlock()
	codec := &identityCodec{}
	func() {
		defer g.work.Done()
		defer c.work.Done()
		if command {
			defer func() { c.mu.Lock(); d.flight = false; d.notifyLocked(); c.mu.Unlock() }()
		}
		options := append([]grpc.CallOption(nil), g.transport.callOptions...)
		options = append(options, grpc.ForceCodec(codec))
		entered = true // Borrowed BeforeDispatch markers cannot undo this evidence.
		rawErr, panicked := identityBorrowInvoke(ctx, g.raw, method, request, response, options)
		if panicked {
			failure = identityFailure{reason: "transport"}
			return
		}
		failure = identitySanitize(rawErr)
		if codec.failed {
			failure = identityFailure{reason: "protocol", category: ErrProtocol}
		}
		if failure.reason == "" {
			failure = classify(codec)
		}
	}()
	// No work count/flight spans application invalidation, including Close reentry.
	if failure.unauthenticated {
		identityInvalidate(g.tokens)
	}
	if failure.reason != "" && i.ctx.Err() != nil {
		failure.cancellation = identityContextFailure(i.ctx.Err()).category
	}
	return
}

func identityBorrowInvoke(ctx context.Context, raw grpc.ClientConnInterface, method string, request, response any, options []grpc.CallOption) (err error, panicked bool) {
	panicked = true
	defer func() { _ = recover() }()
	err = raw.Invoke(ctx, method, request, response, options...)
	panicked = false
	return
}
