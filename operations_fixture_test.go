// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"net"
	"strings"
	"testing"

	jobcontracts "github.com/cratis/chronicle.go/contracts/jobs"
	obscontracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/observation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type operationRPC func(context.Context, string, proto.Message) (proto.Message, error)

func operationsConnection(t *testing.T, call operationRPC) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		return call(ctx, info.FullMethod, request.(proto.Message))
	}))
	jobcontracts.RegisterJobsServer(server, &jobcontracts.UnimplementedJobsServer{})
	obscontracts.RegisterObserversServer(server, &obscontracts.UnimplementedObserversServer{})
	obscontracts.RegisterFailedPartitionsServer(server, &obscontracts.UnimplementedFailedPartitionsServer{})
	sequences.RegisterEventSequencesServer(server, &sequences.UnimplementedEventSequencesServer{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil {
			t.Error(err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///operations", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		server.Stop()
		_ = listener.Close()
		<-done
	})
	return conn
}
func operationServices(t *testing.T, conn grpc.ClientConnInterface) (*observation.Service, *jobs.Service) {
	t.Helper()
	observers, err := observation.New("store", "tenant", conn)
	if err != nil {
		t.Fatal(err)
	}
	jobService, err := jobs.New("store", "tenant", conn)
	if err != nil {
		t.Fatal(err)
	}
	return observers, jobService
}

// inMemoryOperations permits deterministic fake-time deadline/polling tests;
// golden and transport-status cases independently use actual bufconn serialization.
type inMemoryOperations struct {
	grpc.ClientConnInterface
	call operationRPC
}

func (c inMemoryOperations) Invoke(ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	result, err := c.call(ctx, method, args.(proto.Message))
	if err != nil {
		return err
	}
	if result != nil {
		proto.Merge(reply.(proto.Message), result)
	}
	return nil
}
func methodName(method string) string { return method[strings.LastIndex(method, "/")+1:] }
func equalOperation(t *testing.T, got, want proto.Message) {
	t.Helper()
	if !proto.Equal(got, want) {
		t.Fatalf("wire request\ngot: %v\nwant: %v", got, want)
	}
}
