// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/faults"
	contextmetadata "github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthenticationError identifies a rejected OAuth exchange or invalid token.
// Credential values and token response bodies are never included.
type AuthenticationError = connection.AuthenticationError

type generationTransport struct {
	generation  *generation
	callOptions []grpc.CallOption // Frozen client-wide overrides, including borrowed channels.
}

func (t *generationTransport) options(options []grpc.CallOption) []grpc.CallOption {
	if len(t.callOptions) == 0 {
		return options
	}
	// Append without changing caller-owned storage. Client bounds take precedence
	// over service-local options and apply to every unary and streaming RPC.
	return append(append([]grpc.CallOption(nil), options...), t.callOptions...)
}

func authorize(ctx context.Context, source TokenSource) (context.Context, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	if id := contextmetadata.Correlation(ctx); id != (contextmetadata.CorrelationID{}) {
		md.Set("x-correlation-id", id.String())
	}
	if source != nil {
		token, err := source.Token(ctx)
		if err != nil {
			return nil, err
		}
		if token.AccessToken == "" || strings.ContainsAny(token.AccessToken, "\r\n") || (!token.Expiry.IsZero() && !time.Now().Before(token.Expiry)) {
			return nil, &connection.AuthenticationError{}
		}
		md.Set("authorization", "Bearer "+token.AccessToken)
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}

func invalidateRejectedToken(source TokenSource, err error) {
	if status.Code(err) == codes.Unauthenticated {
		if source, ok := source.(TokenInvalidator); ok {
			source.Invalidate()
		}
	}
}

func (t *generationTransport) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	ctx, err := authorize(ctx, t.generation.tokens)
	if err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	// Caller code may ignore cancellation while acquiring a token. The call
	// context follows its generation through an asynchronous AfterFunc bridge,
	// so also check the generation before dispatching after that callback returns.
	if err = ctx.Err(); err == nil {
		err = t.generation.ctx.Err()
	}
	if err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	if err = decision.ValidateDispatch(ctx); err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	if c := t.generation.client; c != nil {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return &faults.BeforeDispatch{Cause: ErrClosed}
		}
		if err := t.generation.ctx.Err(); err != nil {
			c.mu.Unlock()
			return &faults.BeforeDispatch{Cause: err}
		}
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return &faults.BeforeDispatch{Cause: err}
		}
		if err := decision.ValidateDispatch(ctx); err != nil {
			c.mu.Unlock()
			return &faults.BeforeDispatch{Cause: err}
		}
		t.generation.work.Add(1)
		c.work.Add(1)
		c.mu.Unlock()
	}
	err = func() error {
		if c := t.generation.client; c != nil {
			defer t.generation.work.Done()
			defer c.work.Done()
		}
		return t.generation.raw.Invoke(ctx, method, args, reply, t.options(options)...)
	}()
	invalidateRejectedToken(t.generation.tokens, err)
	if err != nil && ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	return err
}

func (t *generationTransport) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, err := authorize(ctx, t.generation.tokens)
	if err != nil {
		return nil, err
	}
	stream, err := t.generation.raw.NewStream(ctx, desc, method, t.options(options)...)
	if err != nil {
		invalidateRejectedToken(t.generation.tokens, err)
		return nil, err
	}
	return &authenticatedStream{ClientStream: stream, source: t.generation.tokens}, nil
}

type authenticatedStream struct {
	grpc.ClientStream
	source TokenSource
}

func (s *authenticatedStream) RecvMsg(message any) error {
	err := s.ClientStream.RecvMsg(message)
	invalidateRejectedToken(s.source, err)
	return err
}
func (s *authenticatedStream) SendMsg(message any) error {
	err := s.ClientStream.SendMsg(message)
	invalidateRejectedToken(s.source, err)
	return err
}
func (s *authenticatedStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	invalidateRejectedToken(s.source, err)
	return err
}
