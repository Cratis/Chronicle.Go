// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"os"

	"github.com/cratis/chronicle.go/contracts"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

type connectAttempt struct {
	done chan struct{}
	err  error
}

// Connect performs structural compatibility preflight and waits for the first
// kernel keep-alive. Concurrent callers share an attempt; the initiating caller's
// deadline bounds it. Canceling startup does not retain that context on the client.
// A lost keep-alive fails subsequent admission; call Connect again explicitly.
// Automatic reconnect/registration replay is not yet supported.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return err
	}
	if c.connected {
		c.mu.Unlock()
		return nil
	}
	if attempt := c.attempt; attempt != nil {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-attempt.done:
			return attempt.err
		}
	}
	attempt := &connectAttempt{done: make(chan struct{})}
	c.attempt = attempt
	c.work.Add(1)
	c.mu.Unlock()
	defer c.work.Done()
	ctx, cancel := context.WithTimeout(ctx, c.config.connectTimeout)
	defer cancel()
	stop := context.AfterFunc(c.life, cancel)
	defer stop()
	err := c.connect(ctx)
	c.mu.Lock()
	attempt.err = err
	c.attempt = nil
	close(attempt.done)
	c.mu.Unlock()
	return err
}

func (c *Client) connect(ctx context.Context) error {
	service := clients.NewConnectionServiceClient(c.transport)
	if !c.config.skipCompatibility {
		response, err := service.CheckCompatibility(ctx, &clients.CompatibilityRequest{
			ClientType: "Go", ClientVersion: "0.1.0-dev", ProtocolVersion: contracts.ProtocolVersion, DescriptorSet: contracts.DescriptorSet(),
		})
		if err != nil {
			return fmt.Errorf("chronicle: compatibility preflight: %w", err)
		}
		if response == nil {
			return ErrProtocol
		}
		if !response.IsCompatible || len(response.Incompatibilities) > 0 {
			return &CompatibilityError{ServerVersion: response.ServerVersion, Details: append([]string(nil), response.Incompatibilities...)}
		}
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("chronicle: connection ID: %w", err)
	}
	streamCtx, cancel := context.WithCancel(c.life)
	stopStartup := context.AfterFunc(ctx, cancel)
	stream, err := service.Connect(streamCtx, &clients.ConnectRequest{ConnectionId: id.String(), ClientVersion: "0.1.0-dev", ClientType: "Go", ProcessId: int32(os.Getpid())})
	if err != nil {
		stopStartup()
		cancel()
		return fmt.Errorf("chronicle: connect: %w", err)
	}
	first, err := stream.Recv()
	if err != nil {
		stopStartup()
		cancel()
		return fmt.Errorf("chronicle: initial keep-alive: %w", err)
	}
	if first.ConnectionId != id.String() {
		stopStartup()
		cancel()
		drainStream(stream)
		return fmt.Errorf("%w: mismatched connection ID", ErrProtocol)
	}
	if _, err = service.ConnectionKeepAlive(ctx, first); err != nil {
		stopStartup()
		cancel()
		drainStream(stream)
		return err
	}
	if !stopStartup() || ctx.Err() != nil {
		cancel()
		drainStream(stream)
		return ctx.Err()
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		drainStream(stream)
		return ErrClosed
	}
	c.connected, c.connectionError = true, nil
	c.work.Add(1)
	c.mu.Unlock()
	go c.keepAlive(streamCtx, cancel, service, stream, id.String())
	return nil
}

func drainStream(stream grpc.ServerStreamingClient[clients.ConnectionKeepAlive]) {
	// Cancellation precedes this join; consume the terminal receive to release admission.
	for {
		if _, err := stream.Recv(); err != nil {
			return
		}
	}
}

func (c *Client) keepAlive(ctx context.Context, cancel context.CancelFunc, service clients.ConnectionServiceClient, stream grpc.ServerStreamingClient[clients.ConnectionKeepAlive], id string) {
	defer c.work.Done()
	defer cancel()
	var failure error
	for {
		message, err := stream.Recv()
		if err != nil {
			failure = err
			break
		}
		if message.ConnectionId != id {
			failure = ErrProtocol
			break
		}
		ackCtx, ackCancel := context.WithTimeout(ctx, c.config.connectTimeout)
		_, err = service.ConnectionKeepAlive(ackCtx, message)
		ackCancel()
		if err != nil {
			failure = err
			break
		}
	}
	cancel()
	drainStream(stream)
	c.mu.Lock()
	c.connected, c.connectionError = false, failure
	c.mu.Unlock()
}
