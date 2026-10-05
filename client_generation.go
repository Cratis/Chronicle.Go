// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cratis/chronicle.go/contracts"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/registration"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

type generation struct {
	client        *Client
	number        uint64
	decisions     bool
	ctx           context.Context
	cancel        context.CancelFunc
	raw           grpc.ClientConnInterface
	closer        io.Closer
	tokens        TokenSource
	oauth         *connection.OAuth
	transport     *generationTransport
	stream        grpc.ServerStreamingClient[clients.ConnectionKeepAlive]
	id            string
	work          sync.WaitGroup
	registrations registration.Cache
	initialStores []*EventStore
	observers     sync.WaitGroup
}

func (c *Client) newGeneration(ctx context.Context) (*generation, error) {
	addresses := make([]string, len(c.uri.addresses))
	for i, address := range c.uri.addresses {
		addresses[i] = address.String()
	}
	var err error
	if c.uri.srv {
		addresses, err = connection.Resolve(ctx, c.config.resolver, c.uri.addresses[0].Host)
		if err != nil {
			return nil, err
		}
	}
	address, err := c.balancer.Next(ctx, addresses)
	if err != nil {
		return nil, err
	}
	g := &generation{client: c, raw: c.config.borrowed, tokens: c.config.tokenSource}
	g.ctx, g.cancel = context.WithCancel(c.life)
	c.nextGeneration++
	g.number = c.nextGeneration
	g.initialStores = c.storeSnapshot()
	if g.tokens == nil && !c.config.noAuth && !c.uri.noAuth {
		g.oauth = connection.NewOAuth(address, c.uri.clientID, c.uri.secret, c.tls)
		g.tokens = g.oauth
	}
	if g.raw == nil {
		var conn *grpc.ClientConn
		conn, err = grpc.NewClient("dns:///"+address, grpc.WithTransportCredentials(credentials.NewTLS(c.tls)),
			grpc.WithDisableRetry(), grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 60 * time.Second, Timeout: 30 * time.Second}),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.config.maxReceiveMessageSize), grpc.MaxCallSendMsgSize(c.config.maxSendMessageSize)))
		if err != nil {
			g.cancel()
			if g.oauth != nil {
				g.oauth.Close()
			}
			return nil, err
		}
		g.raw, g.closer = conn, conn
	}
	g.transport = &generationTransport{generation: g}
	if c.config.maxSendMessageSizeSet {
		g.transport.callOptions = append(g.transport.callOptions, grpc.MaxCallSendMsgSize(c.config.maxSendMessageSize))
	}
	if c.config.maxReceiveMessageSizeSet {
		g.transport.callOptions = append(g.transport.callOptions, grpc.MaxCallRecvMsgSize(c.config.maxReceiveMessageSize))
	}
	return g, nil
}

func (c *Client) establish(ctx context.Context, g *generation) error {
	service := clients.NewConnectionServiceClient(g.transport)
	if !c.config.skipCompatibility {
		response, err := service.CheckCompatibility(ctx, &clients.CompatibilityRequest{ClientType: "Go", ClientVersion: "0.1.0-dev", ProtocolVersion: contracts.ProtocolVersion, DescriptorSet: contracts.DescriptorSet()})
		if err != nil {
			return fmt.Errorf("chronicle: compatibility preflight: %w", err)
		}
		if response == nil {
			return ErrProtocol
		}
		if !response.IsCompatible || len(response.Incompatibilities) > 0 {
			return &CompatibilityError{ServerVersion: response.ServerVersion, Details: append([]string(nil), response.Incompatibilities...)}
		}
		g.decisions = decision.Supported(response.ServerVersion, response.ServerProtocolVersion)
	}
	if c.config.skipKeepAlive {
		// Compatibility is anonymous on the kernel. A protected, read-only RPC
		// verifies credentials without registering a fabricated logical session.
		response, err := service.GetConnectedClients(ctx, &emptypb.Empty{})
		if err != nil {
			return fmt.Errorf("chronicle: skipped-session readiness probe: %w", err)
		}
		if response == nil {
			return ErrProtocol
		}
		return ctx.Err()
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	g.id = id.String()
	stop := context.AfterFunc(ctx, g.cancel)
	defer stop()
	// Only the explicit generation lifetime is retained by the stream, not startup metadata.
	stream, err := service.Connect(g.ctx, &clients.ConnectRequest{ConnectionId: g.id, ClientVersion: "0.1.0-dev", ClientType: "Go", ProcessId: int32(os.Getpid())})
	if err != nil {
		return err
	}
	g.stream = stream
	first, err := stream.Recv()
	if err == nil && (first == nil || first.ConnectionId != g.id) {
		err = ErrProtocol
	}
	if err == nil {
		_, err = service.ConnectionKeepAlive(ctx, first)
	}
	if err == nil && (!stop() || ctx.Err() != nil) {
		err = ctx.Err()
		if err == nil {
			err = context.Canceled
		}
	}
	if err != nil {
		g.cancel()
		drainStream(stream)
	}
	return err
}

func drainStream(stream grpc.ServerStreamingClient[clients.ConnectionKeepAlive]) {
	for {
		if _, err := stream.Recv(); err != nil {
			return
		}
	}
}

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
