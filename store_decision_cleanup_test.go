// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/decision"
	contextmetadata "github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type decisionSessionModels struct {
	readModelKernel
	mu          sync.Mutex
	definitions []*modelcontracts.ReadModelDefinition
	cleanup     func(context.Context, *modelcontracts.DehydrateSessionRequest)
}

func (k *decisionSessionModels) RegisterMany(_ context.Context, request *modelcontracts.RegisterManyRequest) (*emptypb.Empty, error) {
	k.mu.Lock()
	k.definitions = append(k.definitions, request.ReadModels...)
	k.mu.Unlock()
	return &emptypb.Empty{}, nil
}
func (k *decisionSessionModels) GetDefinitions(context.Context, *modelcontracts.GetDefinitionsRequest) (*modelcontracts.GetDefinitionsResponse, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return &modelcontracts.GetDefinitionsResponse{ReadModels: k.definitions}, nil
}
func (k *decisionSessionModels) DehydrateSession(ctx context.Context, request *modelcontracts.DehydrateSessionRequest) (*emptypb.Empty, error) {
	k.cleanup(ctx, request)
	return &emptypb.Empty{}, nil
}

type decisionSessionProjections struct {
	projectionKernel
	mu          sync.Mutex
	definitions []*projectioncontracts.ProjectionDefinition
}

func (k *decisionSessionProjections) Register(_ context.Context, request *projectioncontracts.RegisterRequest) (*emptypb.Empty, error) {
	k.mu.Lock()
	k.definitions = append(k.definitions, request.Projections...)
	k.mu.Unlock()
	return &emptypb.Empty{}, nil
}
func (k *decisionSessionProjections) GetAllDefinitions(context.Context, *projectioncontracts.GetAllDefinitionsRequest) (*projectioncontracts.IEnumerable_ProjectionDefinition, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	return &projectioncontracts.IEnumerable_ProjectionDefinition{Items: k.definitions}, nil
}

type decisionSessionConn struct {
	decisionProfileConn
	cleanups atomic.Int32
}

func (c *decisionSessionConn) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	if _, ok := args.(*sequences.TailSequenceNumberRequest); ok {
		proto.Merge(reply.(proto.Message), &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: 5}})
		return nil
	}
	if _, ok := args.(*modelcontracts.DehydrateSessionRequest); ok {
		c.cleanups.Add(1)
	}
	return c.decisionProfileConn.Invoke(ctx, method, args, reply, options...)
}

func TestDecisionCleanupContextDoesNotBypassClientClosure(t *testing.T) {
	client, err := NewClient(WithNoAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	var calls atomic.Int32
	g := &generation{client: client, ctx: t.Context(), raw: definitionRaw{call: func(context.Context, any) error { calls.Add(1); return nil }}}
	g.transport = &generationTransport{generation: g}
	if err := client.CloseContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	cleanup := decision.CleanupContext(decision.WithDispatchValidation(t.Context(), func() error { return decision.ErrStale }))
	if err := g.transport.Invoke(cleanup, "cleanup", nil, &emptypb.Empty{}); !errors.Is(err, ErrClosed) || calls.Load() != 0 {
		t.Fatal("cleanup bypassed client closure", err, calls.Load())
	}
}

func TestDecisionSessionCleanupDispatchesAfterCallerCancellationOrPublication(t *testing.T) {
	for _, invalidation := range []string{"caller-cancel", "runtime-publication"} {
		t.Run(invalidation, func(t *testing.T) {
			registry, model := projectionRegistry(t)
			models := &decisionSessionModels{}
			projections := &decisionSessionProjections{}
			kernel := &supervisedKernel{readModels: models, projections: projections}
			client, ctx := supervisionClient(t, kernel, WithRegistry(registry), WithTokenSource(decisionTokenSource(func(context.Context) (Token, error) {
				return Token{AccessToken: "fixture-auth"}, nil
			})))
			raw := &decisionSessionConn{decisionProfileConn: decisionProfileConn{ClientConnInterface: client.config.borrowed, version: "19.29.4", protocol: "19.29.4"}}
			client.config.borrowed = raw
			store, err := client.EventStore(ctx, "store", WithNamespace("tenant"))
			if err != nil {
				t.Fatal(err)
			}
			reader := readmodels.DecisionsFor(store.ReadModels(), model)
			if admission := reader.Admit(); !admission.IsAdmitted {
				t.Fatal(admission)
			}
			entered := make(chan *modelcontracts.GetInstanceByKeyRequest, 1)
			release := make(chan struct{})
			releaseFold := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseFold)
			foldDone := make(chan struct{})
			models.get = func(ctx context.Context, request *modelcontracts.GetInstanceByKeyRequest) (*modelcontracts.GetInstanceByKeyResponse, error) {
				defer close(foldDone)
				entered <- proto.CloneOf(request)
				select {
				case <-release:
					return &modelcontracts.GetInstanceByKeyResponse{ReadModel: `{"id":"source","name":"Ada"}`, LastHandledEventSequenceNumber: 5}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			correlation, err := contextmetadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			callCtx, cancel := context.WithCancel(contextmetadata.WithCorrelation(metadata.NewOutgoingContext(ctx, metadata.Pairs("ambient-fixture", "retained")), correlation))
			defer cancel()
			cleaned := make(chan *modelcontracts.DehydrateSessionRequest, 1)
			models.cleanup = func(cleanup context.Context, request *modelcontracts.DehydrateSessionRequest) {
				md, _ := metadata.FromIncomingContext(cleanup)
				if !slices.Equal(md.Get("authorization"), []string{"Bearer fixture-auth"}) || !slices.Equal(md.Get("ambient-fixture"), []string{"retained"}) || !slices.Equal(md.Get("x-correlation-id"), []string{correlation.String()}) {
					t.Error("cleanup lost authentication or ambient metadata")
				}
				deadline, ok := cleanup.Deadline()
				if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 5*time.Second {
					t.Error("cleanup did not have a bounded live deadline")
				}
				cleaned <- proto.CloneOf(request)
			}
			type completion struct {
				read readmodels.DecisionRead[ProjectionModel]
				err  error
			}
			completed := make(chan completion, 1)
			go func() { read, err := reader.GetDetached(callCtx, "source"); completed <- completion{read, err} }()
			var session *modelcontracts.GetInstanceByKeyRequest
			select {
			case session = <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if session.SessionId == "" {
				t.Fatal("fold did not start a session")
			}
			if invalidation == "caller-cancel" {
				cancel()
			} else {
				_, declaration := runtimeModel(t)
				if result, err := store.RegisterProjection(ctx, declaration); err != nil || !result.Published {
					t.Fatal(result, err)
				}
				releaseFold()
			}
			select {
			case finished := <-completed:
				want := error(decision.ErrStale)
				if invalidation == "caller-cancel" {
					want = context.Canceled
				}
				if !errors.Is(finished.err, want) || !finished.read.Token.IsZero() {
					t.Fatal("invalidated read issued evidence", finished)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			awaitSignal(t, ctx, foldDone)
			if raw.cleanups.Load() != 1 {
				t.Fatalf("raw cleanup RPCs = %d, want 1", raw.cleanups.Load())
			}
			select {
			case cleanup := <-cleaned:
				if cleanup.EventStore != session.EventStore || cleanup.Namespace != session.Namespace || cleanup.EventSequenceId != string(events.EventLog) || cleanup.ReadModelIdentifier != session.ReadModelIdentifier || cleanup.ReadModelKey != session.ReadModelKey || cleanup.SessionId != session.SessionId {
					t.Fatal("cleanup changed session coordinates")
				}
			case <-ctx.Done():
				t.Fatal("cleanup did not reach pinned session", ctx.Err())
			}
		})
	}
}
