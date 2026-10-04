// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type enumTrueEvent struct {
	Status projectionEnum `json:"true"`
}
type enumTitleTrueEvent struct {
	Status projectionEnum `json:"True"`
}
type enumFalseEvent struct {
	Status projectionEnum `json:"false"`
}
type enumTitleFalseEvent struct {
	Status projectionEnum `json:"False"`
}
type enumTrueModel struct {
	ID     string         `json:"ID" chronicle:"key"`
	Status projectionEnum `json:"true"`
}
type enumTitleTrueModel struct {
	ID     string         `json:"ID" chronicle:"key"`
	Status projectionEnum `json:"True"`
}
type enumFalseModel struct {
	ID     string         `json:"ID" chronicle:"key"`
	Status projectionEnum `json:"false"`
}
type enumTitleFalseModel struct {
	ID     string         `json:"ID" chronicle:"key"`
	Status projectionEnum `json:"False"`
}
type enumPolicyLiteralEvent struct{ TRUE projectionEnum }
type enumPolicyLiteralModel struct {
	ID   string `json:"ID" chronicle:"key"`
	TRUE projectionEnum
}
type enumSafeLiteralGoName struct {
	ID   string
	True projectionEnum `json:"Status"`
}

func TestEnumAutoMapLiteralPropertyExpressions(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		enumLiteralProperty[enumTrueModel, enumTrueEvent](t, "true", serialization.PreservePropertyNames)
	})
	t.Run("True", func(t *testing.T) {
		enumLiteralProperty[enumTitleTrueModel, enumTitleTrueEvent](t, "True", serialization.PreservePropertyNames)
	})
	t.Run("false", func(t *testing.T) {
		enumLiteralProperty[enumFalseModel, enumFalseEvent](t, "false", serialization.PreservePropertyNames)
	})
	t.Run("False", func(t *testing.T) {
		enumLiteralProperty[enumTitleFalseModel, enumTitleFalseEvent](t, "False", serialization.PreservePropertyNames)
	})
	t.Run("rendered naming policy", func(t *testing.T) {
		enumLiteralProperty[enumPolicyLiteralModel, enumPolicyLiteralEvent](t, "true", serialization.LegacyGoCamelCase)
	})
}

func enumLiteralProperty[M, E any](t *testing.T, path string, policy serialization.NamingPolicy) {
	t.Helper()
	codecs := projectionEnumCodecs(t, false)
	event := mustEvent[E](t, events.WithCodecs(codecs))
	model := mustModel[M](t, readmodels.WithCodecs(codecs))
	for _, handler := range []string{"From", "Join", "EntersOn"} {
		t.Run(handler, func(t *testing.T) {
			options := []projections.Option{}
			if handler == "EntersOn" {
				options = append(options, projections.VariantOf[WorkItem](), projections.VariantKey(projections.Path[M, string]("ID")), projections.EntersOn(event))
			}
			builder := projections.NewBuilder("enum-literal-expression", model, options...)
			switch handler {
			case "From":
				projections.From(builder, event, nil)
			case "Join":
				projections.Join(builder, event, projections.Path[M, string]("ID"), nil)
			}
			_, err := builder.Build()
			if policy == serialization.LegacyGoCamelCase {
				// PreservePropertyNames admits TRUE; only the final rendered
				// source expression true is a kernel literal.
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			enumBoundaryFailure(t, err, path)
		})
	}
	// Model-bound registration reaches the final graph through both explicit
	// subscriptions and generated EntersOn handlers, before connection effects.
	for _, variant := range []bool{false, true} {
		name := "registered From"
		if variant {
			name = "registered EntersOn"
		}
		t.Run(name, func(t *testing.T) {
			r := chronicle.NewRegistry()
			registeredEvent, err := chronicle.RegisterEvent[E](r, events.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			registeredModel, err := chronicle.RegisterReadModel[M](r, readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			options := []projections.Option{projections.FromEvent(registeredEvent)}
			if variant {
				options = []projections.Option{projections.VariantOf[WorkItem](), projections.EntersOn(registeredEvent)}
			}
			if err := r.AddProjection(projections.ModelBound(registeredModel, options...)); err != nil {
				t.Fatal(err)
			}
			enumRegistrationNoRPC(t, r, path, policy)
		})
	}
}

func enumRegistrationNoRPC(t *testing.T, registry *chronicle.Registry, path string, policy serialization.NamingPolicy) {
	t.Helper()
	var unary, streams atomic.Int32
	conn, err := grpc.NewClient("localhost:1", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
			unary.Add(1)
			return errors.New("unexpected RPC")
		}),
		grpc.WithStreamInterceptor(func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, grpc.Streamer, ...grpc.CallOption) (grpc.ClientStream, error) {
			streams.Add(1)
			return nil, errors.New("unexpected stream RPC")
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	client, err := chronicle.Dial(t.Context(), chronicle.WithConnectionString("chronicle://localhost:1"), chronicle.WithGRPCConnection(conn), chronicle.WithRegistry(registry), chronicle.WithNamingPolicy(policy))
	if client != nil {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}
	enumBoundaryFailure(t, err, path)
	if unary.Load() != 0 || streams.Load() != 0 {
		t.Fatalf("invalid declaration issued %d unary / %d stream RPCs", unary.Load(), streams.Load())
	}
}

type enumExcludedLiteralModel struct {
	ID         string         `json:"ID"`
	True       projectionEnum `json:"true" chronicle:"no-auto"`
	TitleTrue  projectionEnum `json:"True" chronicle:"no-auto"`
	False      projectionEnum `json:"false" chronicle:"no-auto"`
	TitleFalse projectionEnum `json:"False" chronicle:"no-auto"`
}
type enumAllLiteralNamesEvent struct {
	True       projectionEnum `json:"true"`
	TitleTrue  projectionEnum `json:"True"`
	False      projectionEnum `json:"false"`
	TitleFalse projectionEnum `json:"False"`
}

func TestEnumAutoMapLiteralNamesHonorExclusionsAndOverrides(t *testing.T) {
	codecs := projectionEnumCodecs(t, false)
	event := mustEvent[enumAllLiteralNamesEvent](t, events.WithCodecs(codecs))
	model := mustModel[enumExcludedLiteralModel](t, readmodels.WithCodecs(codecs))
	for _, join := range []bool{false, true} {
		b := projections.NewBuilder("excluded-enum-literals", model)
		if join {
			projections.Join(b, event, projections.Path[enumExcludedLiteralModel, string]("ID"), nil)
		} else {
			projections.From(b, event, nil)
		}
		if _, err := b.Build(); err != nil {
			t.Fatal(err)
		}
	}
	// An explicit target write suppresses name-AutoMap for both From and Join.
	for _, join := range []bool{false, true} {
		b := projections.NewBuilder("overridden-enum-literal", mustModel[enumTrueModel](t, readmodels.WithCodecs(codecs)))
		e := mustEvent[enumTrueEvent](t, events.WithCodecs(codecs))
		define := func(f *projections.FromBuilder[enumTrueModel, enumTrueEvent]) {
			projections.Value(f, projections.Path[enumTrueModel, projectionEnum]("true"), projectionEnum(1))
		}
		if join {
			projections.Join(b, e, projections.Path[enumTrueModel, string]("ID"), define)
		} else {
			projections.From(b, e, define)
		}
		if _, err := b.Build(); err != nil {
			t.Fatal(err)
		}
	}
	// The Go identifier is not the source expression: this rendered name is safe.
	safe := mustEvent[enumSafeLiteralGoName](t, events.WithCodecs(codecs))
	b := projections.NewBuilder("safe-enum-expression", mustModel[enumSafeLiteralGoName](t, readmodels.WithCodecs(codecs)))
	projections.From(b, safe, nil)
	if _, err := b.Build(); err != nil {
		t.Fatal(err)
	}
}
