// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type changed struct {
	Value string   `json:"value"`
	Items []string `json:"items"`
}

type handler func(context.Context, any) (any, error)

func fixture(t *testing.T, handle handler) (context.Context, *eventsequences.Sequence, *atomic.Int32) {
	t.Helper()
	definition, err := events.Define[changed](events.WithID("changed"), events.WithTags("static"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	calls := &atomic.Int32{}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		calls.Add(1)
		return handle(ctx, request)
	}))
	sequences.RegisterEventSequencesServer(server, &sequences.UnimplementedEventSequencesServer{})
	done := make(chan struct{})
	t.Cleanup(func() {
		server.Stop()
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
		<-done
	})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
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
	})
	sequence, err := eventsequences.New("store", "tenant", "event-log", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx, sequence, calls
}

func success(count int) *sequences.CommandResult_AppendManyResponse {
	positions := make([]uint64, count)
	for i := range positions {
		positions[i] = uint64(i)
	}
	return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: positions, ConcurrencyCheckPerformed: true}}
}

func scope(source string) eventsequences.LabeledScope {
	return eventsequences.LabeledScope{Label: source, Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}
}

func begin(t *testing.T, ctx context.Context, sequence *eventsequences.Sequence) (*transactions.UnitOfWork, *transactions.Owner) {
	t.Helper()
	unit, owner, err := transactions.Begin(ctx, sequence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Rollback(); err != nil {
			t.Error(err)
		}
	})
	return unit, owner
}

func stage(t *testing.T, ctx context.Context, unit *transactions.UnitOfWork, source, value string) {
	t.Helper()
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: events.SourceID(source), Event: changed{Value: value}}}, scope(source)); err != nil {
		t.Fatal(err)
	}
}
