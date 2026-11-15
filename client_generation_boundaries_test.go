// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/contracts/clients"
	"google.golang.org/grpc"
)

type startupBoundaryConn struct {
	grpc.ClientConnInterface
	stream *startupBoundaryStream
	ack    func() error
	acks   int
}

func (c *startupBoundaryConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	c.acks++
	return c.ack()
}

func (c *startupBoundaryConn) NewStream(ctx context.Context, _ *grpc.StreamDesc, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
	c.stream.ctx = ctx
	return c.stream, nil
}

type startupBoundaryStream struct {
	lifecycleStream
	receiveError error
	wrongID      bool
	receives     int
}

func (s *startupBoundaryStream) RecvMsg(message any) error {
	s.receives++
	if s.receives == 1 {
		if s.receiveError != nil {
			return s.receiveError
		}
		if s.wrongID {
			message.(*clients.ConnectionKeepAlive).ConnectionId = "another-connection"
			return nil
		}
	}
	return s.lifecycleStream.RecvMsg(message)
}

func startupFailureCase(name string, cancel context.CancelFunc, failure error) (*startupBoundaryConn, int, error) {
	stream := &startupBoundaryStream{}
	conn := &startupBoundaryConn{stream: stream, ack: func() error { return nil }}
	want, acks := failure, 0
	switch name {
	case "wrong connection":
		stream.wrongID, want = true, ErrProtocol
	case "receive failure":
		stream.receiveError = failure
	case "ack failure":
		conn.ack = func() error { return failure }
		acks = 1
	case "canceled ack":
		conn.ack = func() error { cancel(); return nil }
		want, acks = context.Canceled, 1
	}
	return conn, acks, want
}

func TestEstablishCancelsAndDrainsFailedStartup(t *testing.T) {
	failure := errors.New("startup failure")
	for _, name := range []string{"wrong connection", "receive failure", "ack failure", "canceled ack"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			conn, acks, want := startupFailureCase(name, cancel, failure)
			client := &Client{config: clientConfig{skipCompatibility: true}}
			g := &generation{client: client, raw: conn}
			g.bind(t.Context())
			defer g.cancel()
			g.transport = &generationTransport{generation: g}
			if err := client.establish(ctx, g); !errors.Is(err, want) {
				t.Fatalf("establish error = %v, want %v", err, want)
			}
			if g.ctx.Err() == nil {
				t.Fatal("failed startup did not cancel the generation")
			}
			if conn.acks != acks || conn.stream.receives < 2 {
				t.Fatalf("acks = %d, receives = %d", conn.acks, conn.stream.receives)
			}
		})
	}
}
