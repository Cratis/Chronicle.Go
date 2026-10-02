// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/grpc"
)

func joinClose(err, closeError error) error { return errors.Join(err, closeError) }

// acquire pins a generation before releasing the admission lock. Retirement
// removes it under that lock before joining, so Add cannot race with Wait.
func (c *Client) acquire(ctx context.Context) (*generation, context.Context, func(), error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, nil, ErrClosed
	}
	g := c.current
	if g == nil || g.ctx.Err() != nil {
		err := c.connectionError
		if err == nil {
			err = errors.New("chronicle: connection unavailable")
		}
		c.mu.Unlock()
		return nil, nil, nil, err
	}
	g.work.Add(1)
	c.work.Add(1)
	c.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.ctx, cancel)
	var once sync.Once
	done := func() { once.Do(func() { stop(); cancel(); g.work.Done(); c.work.Done() }) }
	if err := ctx.Err(); err != nil {
		done()
		return nil, nil, nil, err
	}
	return g, ctx, done, nil
}

type clientTransport struct {
	client *Client
	store  *EventStore
}

func (t *clientTransport) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	g, ctx, done, err := t.client.acquire(ctx)
	if err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	defer done()
	if t.store != nil {
		if _, err = t.store.register(ctx, g); err != nil {
			return &faults.BeforeDispatch{Cause: fmt.Errorf("chronicle: registration: %w", err)}
		}
	}
	return g.transport.Invoke(ctx, method, args, reply, options...)
}

func (t *clientTransport) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	g, ctx, done, err := t.client.acquire(ctx)
	if err != nil {
		return nil, err
	}
	if t.store != nil {
		if _, err = t.store.register(ctx, g); err != nil {
			done()
			return nil, err
		}
	}
	stream, err := g.transport.NewStream(ctx, desc, method, options...)
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
