// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/cratis/chronicle.go/internal/connection"
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

type generationTransport struct{ generation *generation }

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
	if err = ctx.Err(); err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	err = t.generation.raw.Invoke(ctx, method, args, reply, options...)
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
	stream, err := t.generation.raw.NewStream(ctx, desc, method, options...)
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
