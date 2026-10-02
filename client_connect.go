// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cratis/chronicle.go/internal/connection"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type supervision struct {
	first   chan struct{}
	done    chan struct{}
	err     error
	failure error
}

// Connect waits for authenticated protocol readiness, not artifact registration.
// Concurrent callers share startup. The initiating context bounds the first attempt,
// never the client lifetime. Transient loss reconnects automatically. A terminal
// auth/compatibility failure is retried only by a subsequent explicit Connect.
func (c *Client) Connect(ctx context.Context) error { return c.connect(ctx, true) }

func (c *Client) connect(ctx context.Context, explicit bool) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.current != nil && c.current.ctx.Err() == nil {
		c.mu.Unlock()
		return nil
	}
	s := c.supervisor
	if s == nil {
		if !explicit && c.connectionError != nil && terminalConnectionError(c.connectionError) {
			err := c.connectionError
			c.mu.Unlock()
			return err
		}
		s = &supervision{first: make(chan struct{}), done: make(chan struct{})}
		c.supervisor = s
		c.connectionError = nil
		c.work.Add(1)
		go c.supervise(ctx, s)
	}
	firstPending := true
	select {
	case <-s.first:
		firstPending = false
	default:
	}
	c.mu.Unlock()
	if !firstPending {
		return c.waitConnected(ctx, s)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.life.Done():
		return ErrClosed
	case <-s.first:
		startupCanceled := errors.Is(s.err, context.Canceled) || errors.Is(s.err, context.DeadlineExceeded)
		if s.err != nil && (ctx.Err() != nil || !startupCanceled) {
			return s.err
		}
	}
	return c.waitConnected(ctx, s)
}

func (c *Client) waitConnected(ctx context.Context, s *supervision) error {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return ErrClosed
		}
		if c.current != nil && c.current.ctx.Err() == nil {
			c.mu.Unlock()
			return nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.life.Done():
			return ErrClosed
		case <-s.done:
			if s.failure != nil {
				return s.failure
			}
			return ErrClosed
		case <-changed:
		}
	}
}

func (c *Client) supervise(startup context.Context, s *supervision) {
	defer c.work.Done()
	defer func() {
		c.mu.Lock()
		c.supervisor = nil
		s.failure = c.connectionError
		close(s.done)
		c.notifyLocked()
		c.mu.Unlock()
	}()
	attempt := 0
	for c.life.Err() == nil {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return
		}
		parent := c.life
		if startup != nil {
			parent = startup
		}
		ctx, cancel := context.WithTimeout(parent, c.config.connectTimeout)
		stop := context.AfterFunc(c.life, cancel)
		g, err := c.newGeneration(ctx)
		if err == nil {
			err = c.establish(ctx, g)
		}
		stop()
		cancel()
		if err == nil && g.ctx.Err() != nil {
			err = g.ctx.Err()
		}
		c.mu.Lock()
		if err == nil && !c.closed {
			c.current = g
			c.connectionError = nil
		} else if err == nil {
			err = ErrClosed
		}
		c.notifyLocked()
		c.mu.Unlock()
		if startup != nil {
			s.err = err
			close(s.first)
			startup = nil
		}
		if err == nil {
			attempt = 0
			err = c.runGeneration(g)
		}
		c.mu.Lock()
		c.current = nil
		c.connectionError = err
		c.notifyLocked()
		c.mu.Unlock()
		if g != nil {
			c.retire(g)
		}
		if terminalConnectionError(err) || c.life.Err() != nil {
			return
		}
		attempt++
		if connection.Wait(c.life, connection.Backoff(attempt, time.Second, 30*time.Second)) != nil {
			return
		}
	}
}

func terminalConnectionError(err error) bool {
	var incompatible *CompatibilityError
	var auth *connection.AuthenticationError
	if errors.As(err, &incompatible) || errors.As(err, &auth) || errors.Is(err, ErrProtocol) || errors.Is(err, ErrClosed) {
		return true
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied, codes.Unimplemented, codes.InvalidArgument:
		return true
	default:
		return false
	}
}

func (c *Client) retire(g *generation) {
	g.cancel()
	if g.stream != nil {
		drainStream(g.stream)
	}
	g.work.Wait()
	// The supervisor still holds c.work while admitting this retirement worker.
	// Slow user handlers cannot prevent the next generation from connecting, but
	// Close/CloseContext still join them before closing the retired channel.
	c.work.Add(1)
	go func() {
		defer c.work.Done()
		c.joinRetired(g)
	}()
}

func (c *Client) joinRetired(g *generation) {
	g.observers.Wait()
	if g.owned {
		if err := g.raw.Close(); err != nil {
			c.mu.Lock()
			c.closeError = errors.Join(c.closeError, fmt.Errorf("chronicle: close channel: %w", err))
			c.mu.Unlock()
		}
	}
	if g.oauth != nil {
		g.oauth.Close()
	}
}
