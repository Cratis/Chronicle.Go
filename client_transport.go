// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/kernelcapability"
	"github.com/cratis/chronicle.go/internal/outgoing"
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
	g, err := c.liveGenerationLocked()
	if err != nil {
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

// liveGenerationLocked inspects connection state without starting or waiting for
// supervision. The caller holds c.mu and has already checked for closure.
func (c *Client) liveGenerationLocked() (*generation, error) {
	if g := c.current; g != nil && g.ctx.Err() == nil {
		return g, nil
	}
	if c.connectionError != nil {
		return nil, c.connectionError
	}
	return nil, errors.New("chronicle: connection unavailable")
}

// selectRegistrationGeneration does not take an RPC lease: registration can
// call application code, which must be able to close the client.
func (c *Client) selectRegistrationGeneration(ctx context.Context) (*generation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requirePreparedLocked("connect", false); err != nil {
		return nil, fmt.Errorf("chronicle: registration: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("chronicle: registration: %w", err)
	}
	return c.liveGenerationLocked()
}

type clientTransport struct {
	client           *Client
	store            *EventStore
	decisionSnapshot *decision.Catalog
}

func (t *clientTransport) OutgoingConfiguration() outgoing.Config { return t.client.config.outgoing }

func (t *clientTransport) AppendOriginResolver() eventsequences.AppendOriginResolver {
	return t.client.config.appendOriginResolver
}

// KernelCapabilities reports the fixes known to be present in the kernel of the
// current generation. It serves only fast pre-checks: a reconnect can change the
// kernel before dispatch, so gated operations also track their needs in the call
// context and generationTransport refuses, before sending, any RPC whose pinned
// generation lacks one.
func (t *clientTransport) KernelCapabilities(ctx context.Context) (kernelcapability.Capabilities, error) {
	g, _, done, err := t.acquire(ctx)
	if err != nil {
		return kernelcapability.Capabilities{}, err
	}
	defer done()
	return g.capabilities, nil
}

func (t *clientTransport) acquire(ctx context.Context) (*generation, context.Context, func(), error) {
	if t.store == nil {
		return t.client.acquire(ctx)
	}
	for {
		g, err := t.client.selectRegistrationGeneration(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := t.store.registerForAdmission(ctx, g); err != nil {
			if g.ctx.Err() != nil && retryReadiness(ctx, err) {
				continue
			}
			return nil, nil, nil, fmt.Errorf("chronicle: registration: %w", err)
		}
		pinned, attemptCtx, done, err := t.client.acquire(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		if pinned == g {
			return pinned, attemptCtx, done, nil
		}
		// A replacement must pass its own registration before dispatch.
		done()
	}
}

func (t *clientTransport) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	g, ctx, done, err := t.acquire(ctx)
	if err != nil {
		return &faults.BeforeDispatch{Cause: err}
	}
	defer done()
	err = g.transport.Invoke(ctx, method, args, reply, options...)
	// Message providers are caller code: release the RPC lease before invoking
	// them, so they cannot deadlock shutdown by retaining their own generation.
	done()
	if err == nil && t.store != nil {
		t.store.resolveConstraintMessages(reply)
	}
	return err
}

func (t *clientTransport) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	g, ctx, done, err := t.acquire(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := g.transport.NewStream(ctx, desc, method, options...)
	if err != nil {
		done()
		return nil, err
	}
	// Once established, a stream owns its lease until completion or context
	// cancellation, even if its caller never receives from it again.
	stop := context.AfterFunc(ctx, done)
	return &ownedStream{ClientStream: stream, done: func() { stop(); done() }}, nil
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
