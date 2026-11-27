// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/cratis/chronicle.go/contracts"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/kernelcapability"
	"github.com/cratis/chronicle.go/internal/registration"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

type generation struct {
	client        *Client
	number        uint64
	decisions     bool
	capabilities  kernelcapability.Capabilities
	ctx           context.Context
	cancel        context.CancelFunc
	lose          func(error) // Ends this generation because the kernel no longer knows its connection ID.
	raw           grpc.ClientConnInterface
	closer        io.Closer
	tokens        TokenSource
	oauth         *connection.OAuth
	transport     *generationTransport
	stream        grpc.ServerStreamingClient[clients.ConnectionKeepAlive]
	id            string
	address       string
	work          sync.WaitGroup
	registrations registration.Cache
	initialStores []*EventStore
	observers     sync.WaitGroup

	registrationWork sync.WaitGroup
}

func (c *Client) newGeneration(ctx context.Context, droppedAddress string) (*generation, error) {
	addresses, err := c.generationAddresses(ctx)
	if err != nil {
		return nil, err
	}
	if c.config.loadBalancer == nil && c.config.borrowed == nil {
		addresses = preferReconnectPeers(addresses, droppedAddress)
	}
	address, err := c.selectAddress(ctx, addresses)
	if err != nil {
		return nil, err
	}
	g := &generation{client: c, raw: c.config.borrowed, tokens: c.config.tokenSource}
	if g.raw == nil {
		g.address = address
	}
	g.bind(c.life)
	c.nextGeneration++
	g.number = c.nextGeneration
	g.initialStores = c.storeSnapshot()
	g.configureAuthentication(address)
	if err := g.openConnection(address); err != nil {
		g.cancel()
		if g.oauth != nil {
			g.oauth.Close()
		}
		return nil, err
	}
	g.configureTransport()
	return g, nil
}

func (c *Client) generationAddresses(ctx context.Context) ([]string, error) {
	addresses := make([]string, len(c.uri.addresses))
	for i, address := range c.uri.addresses {
		addresses[i] = address.String()
	}
	if c.uri.srv {
		return connection.Resolve(ctx, c.config.resolver, c.uri.addresses[0].Host)
	}
	return addresses, nil
}

func (g *generation) logicalSessionAddress() string {
	if g.stream == nil {
		return ""
	}
	return g.address
}

func preferReconnectPeers(addresses []string, droppedAddress string) []string {
	if droppedAddress == "" || len(addresses) <= 1 {
		return addresses
	}
	if !slices.Contains(addresses, droppedAddress) {
		return addresses
	}
	peers := slices.DeleteFunc(slices.Clone(addresses), func(address string) bool { return address == droppedAddress })
	if len(peers) == 0 {
		return addresses
	}
	return peers
}

func (g *generation) configureAuthentication(address string) {
	c := g.client
	if g.tokens != nil {
		return
	}
	// An API key rides on the channel instead, and asking for no authentication beats
	// everything else. Chronicle serves its token endpoint over TLS only, so a
	// cleartext connection without explicit credentials authenticates with nothing.
	if c.config.noAuth || c.uri.noAuth || c.uri.apiKey != "" || (c.uri.disableTLS && !c.uri.explicitCredentials) {
		return
	}
	g.oauth = connection.NewOAuth(address, c.uri.clientID, c.uri.secret, c.tls)
	if c.uri.disableTLS {
		g.oauth.Plaintext()
	}
	g.tokens = g.oauth
}

func (g *generation) openConnection(address string) error {
	if g.raw != nil {
		return nil
	}
	c := g.client
	transport := credentials.NewTLS(c.tls)
	if c.uri.disableTLS {
		transport = insecure.NewCredentials()
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithDisableRetry(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 60 * time.Second, Timeout: 30 * time.Second}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.config.maxReceiveMessageSize), grpc.MaxCallSendMsgSize(c.config.maxSendMessageSize)),
	}
	if c.config.grpcStatsHandler != nil {
		options = append(options, grpc.WithStatsHandler(c.config.grpcStatsHandler))
	}
	if c.uri.apiKey != "" {
		key, err := connection.NewAPIKey(c.uri.apiKey, !c.uri.disableTLS)
		if err != nil {
			return fmt.Errorf("%w: connection string apiKey", ErrInvalidConfiguration)
		}
		options = append(options, grpc.WithPerRPCCredentials(key))
	}
	conn, err := grpc.NewClient("dns:///"+address, options...)
	if err != nil {
		return err
	}
	g.raw, g.closer = conn, conn
	return nil
}

func (g *generation) configureTransport() {
	c := g.client
	g.transport = &generationTransport{generation: g}
	if c.config.maxSendMessageSizeSet {
		g.transport.callOptions = append(g.transport.callOptions, grpc.MaxCallSendMsgSize(c.config.maxSendMessageSize))
	}
	if c.config.maxReceiveMessageSizeSet {
		g.transport.callOptions = append(g.transport.callOptions, grpc.MaxCallRecvMsgSize(c.config.maxReceiveMessageSize))
	}
}

func (c *Client) establish(ctx context.Context, g *generation) error {
	// A lazily connecting gated operation must not gate the connection itself.
	ctx = kernelcapability.Without(ctx)
	service := clients.NewConnectionServiceClient(g.transport)
	if !c.config.skipCompatibility {
		if err := g.checkCompatibility(ctx, service); err != nil {
			return err
		}
	}
	if c.config.skipKeepAlive {
		// Compatibility is anonymous on the kernel. A protected, read-only RPC
		// verifies credentials without registering a fabricated logical session.
		return probeConnectionReadiness(ctx, service)
	}
	return g.connectSession(ctx, service)
}

func (g *generation) checkCompatibility(ctx context.Context, service clients.ConnectionServiceClient) error {
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
	// Skipped verification leaves the zero value: no fix is assumed present.
	g.capabilities = kernelcapability.FromVersion(response.ServerVersion)
	return nil
}

func probeConnectionReadiness(ctx context.Context, service clients.ConnectionServiceClient) error {
	response, err := service.GetConnectedClients(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("chronicle: skipped-session readiness probe: %w", err)
	}
	if response == nil {
		return ErrProtocol
	}
	return ctx.Err()
}

func (g *generation) connectSession(ctx context.Context, service clients.ConnectionServiceClient) error {
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
	err = g.acknowledgeStartup(ctx, service)
	if err == nil {
		err = completeConnectionStartup(ctx, stop)
	}
	if err != nil {
		g.cancel()
		drainStream(stream)
	}
	return err
}

func (g *generation) acknowledgeStartup(ctx context.Context, service clients.ConnectionServiceClient) error {
	first, err := g.stream.Recv()
	if err != nil {
		return err
	}
	if first == nil || first.ConnectionId != g.id {
		return ErrProtocol
	}
	_, err = service.ConnectionKeepAlive(ctx, first)
	return err
}

func completeConnectionStartup(ctx context.Context, stop func() bool) error {
	if stop() && ctx.Err() == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return context.Canceled
}

func drainStream(stream grpc.ServerStreamingClient[clients.ConnectionKeepAlive]) {
	for {
		if _, err := stream.Recv(); err != nil {
			return
		}
	}
}
