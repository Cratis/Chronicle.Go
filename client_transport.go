// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cratis/chronicle.go/internal/faults"
	contextmetadata "github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func joinClose(err, closeError error) error { return errors.Join(err, closeError) }

func (c *Client) admit(ctx context.Context) (context.Context, func(), error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, ErrClosed
	}
	c.work.Add(1)
	c.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.life, cancel)
	var once sync.Once
	done := func() { once.Do(func() { stop(); cancel(); c.work.Done() }) }
	if err := ctx.Err(); err != nil {
		done()
		return nil, nil, err
	}
	return ctx, done, nil
}

func (c *Client) authorize(ctx context.Context) (context.Context, error) {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	if id := contextmetadata.Correlation(ctx); id != (contextmetadata.CorrelationID{}) {
		md.Set("x-correlation-id", id.String())
	}
	if c.tokens != nil {
		token, err := c.tokens.Token(ctx)
		if err != nil {
			return nil, err
		}
		if token.AccessToken == "" || strings.ContainsAny(token.AccessToken, "\r\n") || (!token.Expiry.IsZero() && !time.Now().Before(token.Expiry)) {
			return nil, fmt.Errorf("chronicle: token source returned an empty, invalid or expired credential")
		}
		md.Set("authorization", "Bearer "+token.AccessToken)
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}

type clientTransport struct{ client *Client }

func (t *clientTransport) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	ctx, done, err := t.client.admit(ctx)
	if err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	defer done()
	t.client.mu.Lock()
	connectionError := t.client.connectionError
	t.client.mu.Unlock()
	if connectionError != nil && !strings.HasPrefix(method, "/Cratis.Chronicle.Contracts.Clients.ConnectionService/") {
		return &faults.BeforeDispatch{Cause: fmt.Errorf("chronicle: connection lost; call Connect before resuming: %w", connectionError)}
	}
	ctx, err = t.client.authorize(ctx)
	if err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	if err = ctx.Err(); err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	err = t.client.raw.Invoke(ctx, method, args, reply, options...)
	if err != nil && ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	return err
}

func (t *clientTransport) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, done, err := t.client.admit(ctx)
	if err != nil {
		return nil, err
	}
	ctx, err = t.client.authorize(ctx)
	if err != nil {
		done()
		return nil, err
	}
	stream, err := t.client.raw.NewStream(ctx, desc, method, options...)
	if err != nil {
		done()
		return nil, err
	}
	return &ownedStream{ClientStream: stream, done: done}, nil
}

type ownedStream struct {
	grpc.ClientStream
	done func()
}

func (s *ownedStream) RecvMsg(message any) error {
	err := s.ClientStream.RecvMsg(message)
	if err != nil {
		s.done()
	}
	return err
}
func (s *ownedStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	if err != nil {
		s.done()
	}
	return err
}

func (s *ownedStream) SendMsg(message any) error {
	err := s.ClientStream.SendMsg(message)
	if err != nil {
		s.done()
	}
	return err
}
