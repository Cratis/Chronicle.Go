// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type opened struct {
	Value string `json:"value"`
}
type changed struct {
	Value string `json:"value"`
}
type rpcHandler func(context.Context, any) (any, error)

func sequenceFixture(t *testing.T, handlers map[string]rpcHandler) (*eventsequences.Sequence, *atomic.Int32) {
	t.Helper()
	first, err := events.Define[opened](events.WithID("opened"), events.WithTags("first", "shared"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := events.Define[changed](events.WithID("changed"), events.WithTags("second", "shared"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(first.Descriptor(), second.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return parityFixture(t, handlers, catalog, eventsequences.ConcurrencyPolicy{})
}

type policyConnection struct {
	grpc.ClientConnInterface
	policy   eventsequences.ConcurrencyPolicy
	resolver eventsequences.AppendOriginResolver
}

func (c policyConnection) ConcurrencyPolicy() eventsequences.ConcurrencyPolicy { return c.policy }

func (c policyConnection) AppendOriginResolver() eventsequences.AppendOriginResolver {
	return c.resolver
}

func parityFixture(t *testing.T, handlers map[string]rpcHandler, catalog *events.Catalog, policy eventsequences.ConcurrencyPolicy, resolvers ...eventsequences.AppendOriginResolver) (*eventsequences.Sequence, *atomic.Int32) {
	t.Helper()
	calls := &atomic.Int32{}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		calls.Add(1)
		name := info.FullMethod[strings.LastIndex(info.FullMethod, "/")+1:]
		if handle, ok := handlers[name]; ok {
			return handle(ctx, request)
		}
		return nil, status.Errorf(codes.Unimplemented, "no test handler for %s", name)
	}))
	sequences.RegisterEventSequencesServer(server, &sequences.UnimplementedEventSequencesServer{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil {
			t.Error(err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		server.Stop()
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
		<-done
	})
	var resolver eventsequences.AppendOriginResolver
	if len(resolvers) > 0 {
		resolver = resolvers[0]
	}
	sequence, err := eventsequences.New("store", "tenant", "event-log", catalog, policyConnection{ClientConnInterface: conn, policy: policy, resolver: resolver})
	if err != nil {
		t.Fatal(err)
	}
	return sequence, calls
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func batchSuccess(correlation metadata.CorrelationID, checked bool, count int) *sequences.CommandResult_AppendManyResponse {
	positions := make([]uint64, count)
	for i := range positions {
		positions[i] = uint64(i)
	}
	return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, CorrelationId: wire.Guid(correlation), SequenceNumbers: positions, ConcurrencyCheckPerformed: checked}}
}

func tailResponse(position uint64) *sequences.QueryResult_EventSequenceTailResponse {
	return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: position}}
}

func readEvent(source string, position uint64) *sequences.AppendedEventResponse {
	return &sequences.AppendedEventResponse{Id: "event-id", Content: `{"value":"loaded"}`, OriginalContent: `{"value":"original"}`, Context: &sequences.EventContext{
		EventSourceId: source, EventSourceType: "Default", EventStreamType: "All", EventStreamId: "Default", SequenceNumber: position,
		EventType: &sequences.EventType{Id: "opened", Generation: 1}, Occurred: &sequences.SerializableDateTimeOffset{Value: "2026-01-02T03:04:05.1234567+02:00"}}}
}

func readResponse(data ...*sequences.AppendedEventResponse) *sequences.QueryResult_IEnumerable_AppendedEventResponse {
	return &sequences.QueryResult_IEnumerable_AppendedEventResponse{IsAuthorized: true, Data: data}
}
