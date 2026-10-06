// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"log/slog"

	"github.com/cratis/chronicle.go/internal/diagnostics"
)

// Close immediately cancels client-owned work and joins it. Repeated calls return
// the same cleanup result. Borrowed channels and token sources are never closed.
// Preparation is caller-owned: Close cancels it but does not join its callbacks
// or cleanup. Join Prepare before closing the borrowed scope provider.
// Use CloseContext for a bounded wait when a callback may ignore cancellation.
func (c *Client) Close() error { return c.CloseContext(context.Background()) }

// CloseContext cancels owned work immediately and waits up to ctx's deadline for
// cleanup. A context error means cleanup is incomplete; it continues in the
// background. A later Close/CloseContext can join it. Go cannot kill callbacks.
// Like Close, this does not join caller-owned preparation.
func (c *Client) CloseContext(ctx context.Context) error {
	c.beginShutdown(false)
	c.cancel()
	return c.awaitClose(ctx)
}

// Shutdown stops admission, drains admitted RPCs while ctx permits, then cancels
// streams/supervision and joins owned workers. Deadline expiry cancels remaining
// work and returns ctx.Err(), never a false claim that cleanup completed.
func (c *Client) Shutdown(ctx context.Context) error {
	c.beginShutdown(true)
	return c.awaitClose(ctx)
}

func (c *Client) beginShutdown(graceful bool) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		g, supervisor := c.current, c.supervisor
		c.notifyLocked()
		c.mu.Unlock()
		if !graceful {
			c.cancel()
		}
		go func() {
			if graceful && g != nil {
				g.work.Wait()
			}
			c.cancel()
			if supervisor != nil {
				<-supervisor.done
			}
			c.hooks.finish()
			c.work.Wait()
			if c.balancer != nil {
				c.balancer.Close()
			}
			diagnostics.Log(c.life, c.config.logger, slog.LevelInfo, "client closed", "client", "close", c.closeError)
			close(c.closeDone)
		}()
	})
}

func (c *Client) awaitClose(ctx context.Context) error {
	select {
	case <-c.closeDone:
		return c.closeError
	case <-ctx.Done():
		c.cancel()
		return ctx.Err()
	}
}
