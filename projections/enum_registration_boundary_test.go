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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestEnumInvalidProjectionRegistrationIssuesNoRPC(t *testing.T) {
	for _, variant := range []bool{false, true} {
		name := "From"
		if variant {
			name = "EntersOn"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			conn, err := grpc.NewClient("localhost:1", grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithUnaryInterceptor(func(context.Context, string, any, any, *grpc.ClientConn, grpc.UnaryInvoker, ...grpc.CallOption) error {
					calls.Add(1)
					return errors.New("unexpected RPC")
				}),
				grpc.WithStreamInterceptor(func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, grpc.Streamer, ...grpc.CallOption) (grpc.ClientStream, error) {
					calls.Add(1)
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
			r := chronicle.NewRegistry()
			event, err := chronicle.RegisterEvent[enumIntegerCaseEvent](r)
			if err != nil {
				t.Fatal(err)
			}
			model, err := chronicle.RegisterReadModel[enumVariantModel](r, readmodels.WithCodecs(projectionEnumCodecs(t, false)))
			if err != nil {
				t.Fatal(err)
			}
			options := []projections.Option{projections.FromEvent(event)}
			if variant {
				options = []projections.Option{projections.VariantOf[WorkItem](), projections.EntersOn(event)}
			}
			if err := r.AddProjection(projections.ModelBound(model, options...)); err != nil {
				t.Fatal(err)
			}
			client, err := chronicle.Dial(t.Context(), chronicle.WithConnectionString("chronicle://localhost:1"), chronicle.WithGRPCConnection(conn), chronicle.WithRegistry(r))
			if client != nil {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			}
			enumBoundaryFailure(t, err, "Status")
			if calls.Load() != 0 {
				t.Fatalf("invalid projection issued %d RPCs", calls.Load())
			}
		})
	}
}

// This named integer is deliberately unregistered. Its name does not select an
// enum codec, and valid ordinary integer literals retain their behavior.
type Unknown int32

type ordinaryUnknownView struct{ Value Unknown }

func TestEnumLiteralTargetValidationPreservesOrdinaryNamedIntegers(t *testing.T) {
	event := mustEvent[IssueCreated](t, events.WithID("ordinary-literal"))
	for _, path := range []string{"Missing", "Value"} {
		b := projections.NewBuilder("ordinary-literal", mustModel[ordinaryUnknownView](t), projections.NoAutoMap())
		projections.From(b, event, func(f *projections.FromBuilder[ordinaryUnknownView, IssueCreated]) {
			projections.Value(f, projections.Path[ordinaryUnknownView, Unknown](path), Unknown(1))
		})
		_, err := b.Build()
		if path == "Missing" {
			enumBoundaryFailure(t, err, path)
		} else if err != nil {
			t.Fatalf("valid ordinary named-integer literal: %v", err)
		}
	}
}
