// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type noSinkCustomModel struct {
	ID   string `chronicle:"key"`
	Name string `chronicle:"set(sinkDefaultEvent)"`
}

type noSinkJoinedModel struct {
	ID   string `chronicle:"key"`
	Name string `chronicle:"join(sinkDefaultEvent,on=Name)"`
}

func noSinkBound[M any](t *testing.T, r *Registry) projections.Declaration {
	t.Helper()
	model, err := RegisterReadModel[M](r)
	if err != nil {
		t.Fatal(err)
	}
	return projections.ModelBound(model)
}

func noSinkBoundCustom(t *testing.T, r *Registry) projections.Declaration {
	t.Helper()
	model, err := RegisterReadModel[noSinkCustomModel](r)
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[sinkDefaultEvent]()
	if err != nil {
		t.Fatal(err)
	}
	return projections.ModelBound(model, projections.FromEvent(event, projections.UsingKey(projections.Path[sinkDefaultEvent, string]("Name"))))
}

func noSinkFluent(t *testing.T, r *Registry) projections.Declaration {
	t.Helper()
	model, err := RegisterReadModel[sinkDefaultModel](r)
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[sinkDefaultEvent]()
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("custom-key", model)
	projections.From(builder, event, func(from *projections.FromBuilder[sinkDefaultModel, sinkDefaultEvent]) {
		projections.Map(from, projections.Path[sinkDefaultModel, string]("Name"), projections.Path[sinkDefaultEvent, string]("Name"))
	}, projections.UsingKey(projections.Path[sinkDefaultEvent, string]("Name")))
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return declaration
}

func TestNoSinkPreparationRefusesUnsupportedStaticAndFactoryShapes(t *testing.T) {
	for name, declare := range map[string]func(*testing.T, *Registry) projections.Declaration{
		"model-bound key":  noSinkBoundCustom,
		"model-bound join": noSinkBound[noSinkJoinedModel],
		"fluent key":       noSinkFluent,
	} {
		for _, factory := range []bool{false, true} {
			t.Run(name+"/factory="+boolName(factory), func(t *testing.T) {
				noSinkPreparationBoundary(t, declare, factory)
			})
		}
	}
}

func TestRuntimeProjectionPreparationUsesEffectiveNoneShape(t *testing.T) {
	registry := NewRegistry()
	event, err := RegisterEvent[sinkDefaultEvent](registry)
	if err != nil {
		t.Fatal(err)
	}
	custom := noSinkFluent(t, registry)
	client, err := NewClient(WithRegistry(registry), WithDefaultSinkType(readmodels.NoSink))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	store := &EventStore{storeOwner: &storeOwner{client: client, name: "store"}}
	source := projections.ModelBoundDescriptor(custom.Model(), projections.FromEvent(event))
	definition, err := store.prepareProjectionAddition(source)
	if err != nil {
		t.Fatal("source-keyed runtime None refused", err)
	}
	if definition.Model().Sink().Type != readmodels.NoSink || definition.IsPassive() {
		t.Fatal("runtime None selection changed")
	}
	refused, err := store.prepareProjectionAddition(custom)
	if !errors.Is(err, ErrInvalidConfiguration) || refused.KernelDefinition() != nil {
		t.Fatal("custom-key runtime None accepted", err)
	}
}

func boolName(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func noSinkPreparationBoundary(t *testing.T, declare func(*testing.T, *Registry) projections.Declaration, factory bool) {
	t.Helper()
	r := NewRegistry()
	if _, err := RegisterEvent[sinkDefaultEvent](r); err != nil {
		t.Fatal(err)
	}
	declaration := declare(t, r)
	calls := 0
	if factory {
		if err := RegisterProjectionFactory(r, declaration.Identifier(), declaration.Model(), func() *sinkFactory { return &sinkFactory{} }, func(context.Context, *sinkFactory) (projections.Declaration, error) {
			calls++
			return declaration, nil
		}); err != nil {
			t.Fatal(err)
		}
	} else if err := r.AddProjection(declaration); err != nil {
		t.Fatal(err)
	}
	noSinkPreparedRefusal(t, r, factory, &calls)
	noSinkMaterializedControls(t, r)
	noSinkDialRefusal(t, r)
}

func noSinkPreparedRefusal(t *testing.T, r *Registry, factory bool, calls *int) {
	t.Helper()
	preparation := captureForTest(t, WithRegistry(r), WithDefaultSinkType(readmodels.NoSink))
	if *calls != 0 {
		t.Fatal("capture invoked factory")
	}
	client, err := preparation.Prepare(t.Context(), nil)
	if client != nil || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("unsupported shape prepared: client=%v error=%v", client, err)
	}
	if factory && *calls != 1 {
		t.Fatal("factory preparation repeated", *calls)
	}
}

func noSinkMaterializedControls(t *testing.T, r *Registry) {
	t.Helper()
	for _, kind := range []readmodels.SinkType{readmodels.MongoDB, readmodels.SQL, readmodels.InMemory} {
		control, err := NewClient(WithRegistry(r), WithDefaultSinkType(kind))
		if err != nil {
			t.Fatal("materialized control rejected", err)
		}
		if err := control.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func noSinkDialRefusal(t *testing.T, r *Registry) {
	t.Helper()
	var dials, rpcs atomic.Int32
	conn, err := grpc.NewClient("localhost:1", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("unexpected dial")
		}),
		grpc.WithUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
			rpcs.Add(1)
			return errors.New("unexpected RPC")
		}),
		grpc.WithStreamInterceptor(func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, grpc.Streamer, ...grpc.CallOption) (grpc.ClientStream, error) {
			rpcs.Add(1)
			return nil, errors.New("unexpected stream")
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client, err := Dial(t.Context(), WithRegistry(r), WithDefaultSinkType(readmodels.NoSink), WithGRPCConnection(conn))
	if client != nil || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("unsupported shape dialed: client=%v error=%v", client, err)
	}
	if dials.Load() != 0 || rpcs.Load() != 0 {
		t.Fatalf("shape reached transport: dials=%d rpcs=%d", dials.Load(), rpcs.Load())
	}
}
