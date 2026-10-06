// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"fmt"
)

func (c *Client) retire(g *generation) {
	c.mu.Lock()
	g.cancel()
	c.mu.Unlock()
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
	if g.closer != nil {
		if err := g.closer.Close(); err != nil {
			c.mu.Lock()
			c.closeError = errors.Join(c.closeError, fmt.Errorf("chronicle: close channel: %w", err))
			c.mu.Unlock()
		}
	}
	if g.oauth != nil {
		g.oauth.Close()
	}
}
