// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
)

func TestReleaseRejectsWrongProtectedScalarKind(t *testing.T) {
	model := person(t, readmodels.WithPII("count"))
	service, ctx := serviceFixture(t, &modelKernel{release: func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		return &compliance.ReleaseResponse{Payload: `{"count":"42"}`}, nil
	}}, model.Descriptor())
	value, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"id":"owner","count":"ciphertext"}`))
	if value != nil || !errors.Is(err, readmodels.ErrRelease) {
		t.Fatal("wrong released scalar kind was accepted")
	}
}

func TestReleaseRejectsOmittedNestedProtectedValues(t *testing.T) {
	type Contact struct {
		Email string `json:"email" chronicle:"pii"`
	}
	type Model struct {
		ID       string    `json:"id"`
		Contact  Contact   `json:"contact"`
		Children []Contact `json:"children"`
	}
	model, err := readmodels.Define[Model]()
	if err != nil {
		t.Fatal(err)
	}
	for _, reply := range []string{`{"contact":{},"children":[{"email":"clear"}]}`, `{"contact":{"email":"clear"},"children":[]}`, `{"contact":{"email":"clear"},"children":[{}]}`} {
		service, ctx := serviceFixture(t, &modelKernel{release: func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
			return &compliance.ReleaseResponse{Payload: reply}, nil
		}}, model.Descriptor())
		result, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"id":"owner","contact":{"email":"ciphertext"},"children":[{"email":"ciphertext"}]}`))
		if result != nil || !errors.Is(err, readmodels.ErrRelease) {
			t.Fatal("incomplete nested release escaped")
		}
	}
}

func TestReleaseDoesNotInferSubjectFromSerializedProjectionKey(t *testing.T) {
	type Model struct {
		Key   string `json:"id" chronicle:"key"`
		Value string `json:"value" chronicle:"pii"`
	}
	model, err := readmodels.Define[Model]()
	if err != nil {
		t.Fatal(err)
	}
	service, ctx := serviceFixture(t, &modelKernel{release: func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		t.Error("key used as subject")
		return nil, nil
	}}, model.Descriptor())
	if value, err := readmodels.For(service, model).Release(ctx, Model{Key: "not-owner", Value: "ciphertext"}); value.Value != "" || !errors.Is(err, readmodels.ErrRelease) {
		t.Fatal("projection key was interpreted as Go ID")
	}
}

func TestBooleanReleaseSubjectMatchesCSharpToString(t *testing.T) {
	type Model struct {
		Owner bool   `json:"owner" chronicle:"subject"`
		Value string `json:"value" chronicle:"pii"`
	}
	model, err := readmodels.Define[Model]()
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan string, 2)
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		requests <- r.Subject
		return &compliance.ReleaseResponse{Payload: r.Payload}, nil
	}}, model.Descriptor())
	for _, owner := range []bool{true, false} {
		if _, err := readmodels.For(service, model).Release(ctx, Model{Owner: owner, Value: "display"}); err != nil {
			t.Fatal(err)
		}
		want := "False"
		if owner {
			want = "True"
		}
		if got := <-requests; got != want {
			t.Fatal("boolean subject spelling differs")
		}
	}
}

func TestConfidentialityWatchesAndWindowsReleaseWithoutSubject(t *testing.T) {
	for _, window := range []bool{false, true} {
		t.Run(map[bool]string{false: "changes", true: "windows"}[window], func(t *testing.T) {
			model, err := readmodels.Define[IndependentSecrets](readmodels.WithObserver(readmodels.Projection, "secrets"))
			if err != nil {
				t.Fatal(err)
			}
			kernel := &watchKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
				if r.Subject != "" {
					t.Error("independent value acquired an owner")
				}
				return &compliance.ReleaseResponse{HasError: true}, nil
			}}
			kernel.watch = func(_ *contracts.WatchRequest, s grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
				if err := s.Send(&contracts.ReadModelChangeset{Subscribed: true}); err != nil {
					return err
				}
				if err := s.Send(sendChange("tenant-a", contracts.ReadModelChangeType_Added, `{"shared":"ciphertext","global":"ciphertext"}`)); err != nil {
					return err
				}
				<-s.Context().Done()
				return s.Context().Err()
			}
			kernel.observe = func(_ *contracts.ObserveInstancesRequest, s grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error {
				if err := s.Send(&contracts.ObserveInstancesResponse{Instances: []string{`{"shared":"ciphertext","global":"ciphertext"}`}}); err != nil {
					return err
				}
				<-s.Context().Done()
				return s.Context().Err()
			}
			service, ctx := watchFixture(t, kernel, model.Descriptor())
			reader := readmodels.For(service, model)
			if window {
				sub, err := reader.Materialized().ObserveInstances(ctx, nil)
				if sub != nil {
					if closeErr := sub.Close(); closeErr != nil {
						t.Error(closeErr)
					}
				}
				if sub != nil || !errors.Is(err, readmodels.ErrRelease) {
					t.Fatal("unsafe encrypted window")
				}
			} else {
				sub, err := reader.Watch(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := sub.Close(); err != nil {
						t.Error(err)
					}
				}()
				value, err := sub.Recv()
				if value.HasValue || !errors.Is(err, readmodels.ErrRelease) {
					t.Fatal("unsafe encrypted change")
				}
			}
		})
	}
}
