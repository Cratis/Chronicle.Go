// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/internal/connection"
)

func (c *Client) runGeneration(g *generation) error {
	if c.config.skipKeepAlive {
		registered := make(chan struct{})
		go func() { defer close(registered); c.replayRegistrations(g) }()
		<-g.ctx.Done()
		<-registered
		return g.ctx.Err()
	}
	heartbeat := make(chan struct{}, 1)
	received := make(chan error, 1)
	registered := make(chan struct{})
	receiverDone := make(chan struct{})
	go func() {
		defer close(receiverDone)
		received <- c.receiveKeepAlive(g, heartbeat)
		g.cancel()
		drainStream(g.stream)
	}()
	go func() { defer close(registered); c.replayRegistrations(g) }()
	err := connection.Watch(g.ctx, c.config.keepAliveTimeout, heartbeat, received)
	g.cancel()
	// Watch always leaves joining the receiver to its owner.
	<-receiverDone
	<-registered
	return err
}

func (c *Client) receiveKeepAlive(g *generation, heartbeat chan<- struct{}) error {
	service := clients.NewConnectionServiceClient(g.transport)
	for {
		message, err := g.stream.Recv()
		if err != nil {
			return err
		}
		if message == nil || message.ConnectionId != g.id {
			return ErrProtocol
		}
		select {
		case heartbeat <- struct{}{}:
		default:
		}
		ctx, cancel := context.WithTimeout(g.ctx, c.config.connectTimeout)
		_, err = service.ConnectionKeepAlive(ctx, message)
		cancel()
		if err != nil {
			return err
		}
	}
}
