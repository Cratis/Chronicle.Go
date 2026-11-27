// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/diagnostics"
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
// WithSkipKeepAlive uses a protected read-only startup probe instead of a logical
// connection session; no application-heartbeat-driven reconnect or registration
// replay after kernel restarts is guaranteed in that mode.
func (c *Client) Connect(ctx context.Context) error { return c.connect(ctx, true) }

func (c *Client) connect(ctx context.Context, explicit bool) error {
	s, firstPending, err := c.connectSupervisor(ctx, explicit)
	if err != nil || s == nil {
		return err
	}
	if !firstPending {
		return c.waitConnected(ctx, s)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.life.Done():
		return ErrClosed
	case <-s.first:
		if err := connectionStartupError(ctx, s.err); err != nil {
			return err
		}
	}
	return c.waitConnected(ctx, s)
}

func (c *Client) connectSupervisor(ctx context.Context, explicit bool) (*supervision, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requirePreparedLocked("connect", false); err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if c.current != nil && c.current.ctx.Err() == nil {
		return nil, false, nil
	}
	s, err := c.ensureSupervisorLocked(ctx, explicit)
	if err != nil {
		return nil, false, err
	}
	firstPending := true
	select {
	case <-s.first:
		firstPending = false
	default:
	}
	return s, firstPending, nil
}

func (c *Client) ensureSupervisorLocked(ctx context.Context, explicit bool) (*supervision, error) {
	if c.supervisor != nil {
		return c.supervisor, nil
	}
	if !explicit && c.connectionError != nil {
		if terminalConnectionError(c.connectionError) {
			return nil, c.connectionError
		}
	}
	s := &supervision{first: make(chan struct{}), done: make(chan struct{})}
	c.supervisor = s
	c.connectionError = nil
	c.work.Add(1)
	go c.supervise(ctx, s)
	return s, nil
}

func connectionStartupError(ctx context.Context, err error) error {
	startupCanceled := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil || !startupCanceled {
		return err
	}
	return nil
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
	c.hooks.start(c)
	defer func() {
		c.mu.Lock()
		c.supervisor = nil
		s.failure = c.connectionError
		close(s.done)
		c.notifyLocked()
		c.mu.Unlock()
	}()
	attempt := 0
	var droppedAddress string
	for c.life.Err() == nil {
		c.mu.Lock()
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return
		}
		g, err := c.establishGeneration(startup, droppedAddress)
		droppedAddress = "" // Prefer peers for one attempt, even if establishment fails.
		err = c.installGeneration(g, err)
		if startup != nil {
			s.err = err
			close(s.first)
			startup = nil
		}
		if err == nil {
			attempt = 0
			err = c.runConnectedGeneration(g)
			droppedAddress = g.logicalSessionAddress()
		}
		c.endGeneration(g, err)
		if c.life.Err() != nil {
			return
		}
		diagnostics.Log(c.life, c.config.logger, slog.LevelWarn, "client connection ended", "client", "connect", err)
		if terminalConnectionError(err) {
			return
		}
		diagnostics.Log(c.life, c.config.logger, slog.LevelInfo, "client reconnect scheduled", "client", "reconnect", nil)
		attempt++
		if connection.Wait(c.life, connection.Backoff(attempt, time.Second, 30*time.Second)) != nil {
			return
		}
	}
}

func (c *Client) establishGeneration(startup context.Context, droppedAddress string) (*generation, error) {
	parent := c.life
	if startup != nil {
		parent = startup
	}
	ctx, cancel := context.WithTimeout(parent, c.config.connectTimeout)
	stop := context.AfterFunc(c.life, cancel)
	g, err := c.newGeneration(ctx, droppedAddress)
	if err == nil {
		err = c.establish(ctx, g)
	}
	stop()
	cancel()
	if err == nil && g.ctx.Err() != nil {
		err = g.ctx.Err()
	}
	return g, err
}

func (c *Client) installGeneration(g *generation, err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil && !c.closed {
		c.current = g
		c.connectionError = nil
	} else if err == nil {
		err = ErrClosed
	}
	c.notifyLocked()
	return err
}

func (c *Client) runConnectedGeneration(g *generation) error {
	c.hooks.publishConnected(g)
	diagnostics.Log(c.life, c.config.logger, slog.LevelInfo, "client connected", "client", "connect", nil)
	err := c.runGeneration(g)
	cause := err
	if c.life.Err() != nil {
		cause = ErrClosed
	}
	c.hooks.publishDisconnected(g, cause)
	return err
}

func (c *Client) endGeneration(g *generation, err error) {
	c.mu.Lock()
	c.current = nil
	c.connectionError = err
	c.notifyLocked()
	c.mu.Unlock()
	if g != nil {
		c.retire(g)
	}
}
