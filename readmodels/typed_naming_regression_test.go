// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/types/known/emptypb"
)

type plannedTypedModel struct {
	ID         string
	ExternalID string               `json:"ID"`
	Name       conceptfixtures.Name `chronicle:"pii"`
	Items      []string
	Optional   *[]string
}

func TestTypedReadsUseFrozenNamingPlan(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		t.Run([]string{"preserve", "camel", "legacy"}[policy], func(t *testing.T) {
			registry := chronicle.NewRegistry()
			model, err := chronicle.RegisterReadModel[plannedTypedModel](registry, readmodels.WithObserver(readmodels.Projection, "planned"))
			if err != nil {
				t.Fatal(err)
			}
			client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithNamingPolicy(policy))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
			artifacts, err := client.Artifacts("store")
			if err != nil {
				t.Fatal(err)
			}
			d, _ := artifacts.ReadModels.LookupIdentifier(model.Identifier())
			want := plannedTypedModel{ID: "person", ExternalID: "external", Name: "Ada", Items: []string{}}
			data, err := d.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			// Freeze the exact serialized spellings, including the two distinct IDs.
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			if string(fields[d.KeyProperty()]) != `"person"` || string(fields["ID"]) != `"external"` {
				t.Fatal(string(data))
			}
			var releaseFailure atomic.Bool
			kernel := &modelKernel{
				kernel: protectedReleaseKernel,
				get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
					return &contracts.GetInstanceByKeyResponse{ReadModel: string(data), LastHandledEventSequenceNumber: 7}, nil
				},
				release: func(_ context.Context, request *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
					if releaseFailure.Load() {
						return &compliance.ReleaseResponse{HasError: true}, nil
					}
					if request.Subject != "person" || request.Schema != d.Schema() {
						t.Error("release lost frozen metadata")
					}
					return &compliance.ReleaseResponse{Payload: request.Payload}, nil
				},
				dehydrate: func(context.Context, *contracts.DehydrateSessionRequest) (*emptypb.Empty, error) {
					return &emptypb.Empty{}, nil
				},
			}
			service, ctx := serviceFixture(t, kernel, d)
			reader := readmodels.For(service, model)
			t.Run("Get", func(t *testing.T) {
				got, err := reader.Get(ctx, "person")
				if err != nil || !got.Exists || got.LastHandled == nil || *got.LastHandled != 7 || !reflect.DeepEqual(got.Value, want) {
					t.Fatalf("Get = %+v, %v; want %+v", got, err, want)
				}
			})
			t.Run("session", func(t *testing.T) {
				session, err := reader.NewSession("person")
				if err != nil {
					t.Fatal(err)
				}
				got, err := session.Get(ctx)
				if closeErr := session.Close(ctx); closeErr != nil {
					t.Fatal(closeErr)
				}
				if err != nil || !got.Exists || !reflect.DeepEqual(got.Value, want) {
					t.Fatalf("session Get = %+v, %v; want %+v", got, err, want)
				}
			})
			for _, typ := range []reflect.Type{reflect.TypeFor[plannedTypedModel](), reflect.TypeFor[*plannedTypedModel]()} {
				t.Run("injection "+typ.String(), func(t *testing.T) {
					got, err := service.GetValue(ctx, typ, "person")
					expected := any(want)
					if typ.Kind() == reflect.Pointer {
						expected = &want
					}
					if err != nil || !reflect.DeepEqual(got, expected) {
						t.Fatalf("injection = %+v, %v; want %+v", got, err, expected)
					}
				})
			}
			t.Run("Release", func(t *testing.T) {
				got, err := reader.Release(ctx, want)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("Release = %+v, %v", got, err)
				}
				releaseFailure.Store(true)
				got, err = reader.Release(ctx, want)
				if !errors.Is(err, readmodels.ErrRelease) || !errors.Is(err, chronicle.ErrProtocol) || !reflect.DeepEqual(got, plannedTypedModel{}) {
					t.Fatalf("failed Release = %+v, %v", got, err)
				}
			})
		})
	}
}
