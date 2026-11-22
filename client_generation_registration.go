// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "context"

// startRegistration owns a definition pass independently of any RPC waiter.
// Admission shares retirement's lock, so registrationWork.Add cannot race Wait.
// Unlike raw RPC work, these workers are canceled before being joined during
// graceful shutdown; an abandoned waiter must not keep registration alive then.
func (c *Client) startRegistration(g *generation, run func(context.Context)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if err := g.ctx.Err(); err != nil {
		return err
	}
	g.registrationWork.Add(1)
	c.work.Add(1)
	go func() {
		defer g.registrationWork.Done()
		defer c.work.Done()
		run(g.ctx)
	}()
	return nil
}
