// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestReadModelSessionRetainsCoordinatesAndRetriesFailedCleanup(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Projection, "people"), readmodels.WithEventSequence("outbox"))
	reads := make(chan *contracts.GetInstanceByKeyRequest, 2)
	var cleanups atomic.Int32
	var sessionID string
	service, ctx := serviceFixture(t, &modelKernel{
		get: func(_ context.Context, r *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
			reads <- r
			return &contracts.GetInstanceByKeyResponse{ReadModel: "null", LastHandledEventSequenceNumber: ^uint64(0)}, nil
		},
		dehydrate: func(_ context.Context, r *contracts.DehydrateSessionRequest) (*emptypb.Empty, error) {
			if r.EventStore != "store" || r.Namespace != "tenant-a" || r.ReadModelKey != "key" || r.EventSequenceId != "outbox" || r.ReadModelIdentifier != string(model.Identifier()) || r.SessionId != sessionID {
				t.Errorf("cleanup coordinates: %+v", r)
			}
			if cleanups.Add(1) == 1 {
				return nil, status.Error(codes.Unavailable, "retry cleanup")
			}
			return &emptypb.Empty{}, nil
		},
	}, model.Descriptor())
	session, err := readmodels.For(service, model).NewSession("key")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result, err := session.Get(ctx)
		if err != nil || result.Exists {
			t.Fatalf("%+v %v", result, err)
		}
		request := <-reads
		id, err := uuid.Parse(request.SessionId)
		if err != nil || id == uuid.Nil {
			t.Fatal("invalid generated session ID")
		}
		if sessionID != "" && sessionID != request.SessionId {
			t.Fatal("session not reused")
		}
		sessionID = request.SessionId
	}
	if err = session.Close(ctx); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	if _, err = session.Get(ctx); !errors.Is(err, chronicle.ErrClosed) {
		t.Fatal(err)
	}
	if err = session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(ctx); err != nil || cleanups.Load() != 2 {
		t.Fatal("cleanup not idempotent")
	}
}

func TestSessionCancellationStillRequiresDehydration(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Projection, "people"))
	entered := make(chan struct{})
	var cleanups atomic.Int32
	service, ctx := serviceFixture(t, &modelKernel{
		get: func(ctx context.Context, _ *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		dehydrate: func(context.Context, *contracts.DehydrateSessionRequest) (*emptypb.Empty, error) {
			cleanups.Add(1)
			return &emptypb.Empty{}, nil
		},
	}, model.Descriptor())
	session, err := readmodels.For(service, model).NewSession("key")
	if err != nil {
		t.Fatal(err)
	}
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := session.Get(readCtx); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err = session.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = session.Close(ctx); err != nil || cleanups.Load() != 1 {
		t.Fatalf("cleanup: %v", err)
	}
}

func TestUnsupportedSessionsFailBeforeDispatch(t *testing.T) {
	for _, model := range []readmodels.Model[Person]{person(t), person(t, readmodels.WithObserver(readmodels.Reducer, "reducer"))} {
		service, _ := serviceFixture(t, &modelKernel{}, model.Descriptor())
		if _, err := readmodels.For(service, model).NewSession("key"); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal(err)
		}
	}
}
