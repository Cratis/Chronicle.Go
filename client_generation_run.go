// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/internal/connection"
)

func (c *Client) runGeneration(g *generation) error {
	if c.config.skipKeepAlive {
		registered := make(chan struct{})
		go func() { defer close(registered); c.replayRegistrations(g) }()
		<-g.ctx.Done()
		<-registered
		return generationEnded(g, g.ctx.Err())
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
	return generationEnded(g, err)
}

// generationEnded reports a session the kernel no longer knows as ErrSessionLost,
// wrapping the kernel status that reported it, rather than the cancellation or
// stream error it caused.
func generationEnded(g *generation, err error) error {
	if cause := context.Cause(g.ctx); errors.Is(cause, ErrSessionLost) {
		return cause
	}
	return err
}

// bind gives the generation its lifetime under parent.
func (g *generation) bind(parent context.Context) {
	var cancel context.CancelCauseFunc
	g.ctx, cancel = context.WithCancelCause(parent)
	g.cancel = func() { cancel(nil) }
	g.lose = func(kernel error) { cancel(fmt.Errorf("%w: %w", ErrSessionLost, kernel)) }
}

// loseSession ends this generation because the kernel no longer knows its
// connection ID. The supervisor then reconnects under a new connection ID. A
// report from an observer of an already ended generation changes nothing, so
// many observers failing together cause one reconnect.
func (g *generation) loseSession(kernel error) {
	if g.lose != nil {
		g.lose(kernel)
	}
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
