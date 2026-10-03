// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/types/known/emptypb"
)

type IndependentSecrets struct {
	Shared string `json:"shared" chronicle:"encrypted(scope=namespace)"`
	Global string `json:"global" chronicle:"encrypted(scope=global)"`
}

func TestSubjectIndependentReleaseAndCollections(t *testing.T) {
	model, err := readmodels.Define[IndependentSecrets]()
	if err != nil {
		t.Fatal(err)
	}
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		if r.Subject != "" || !strings.Contains(r.Schema, `"security"`) || strings.Contains(r.Schema, `"compliance"`) {
			t.Error("independent confidentiality has wrong wire metadata")
		}
		if strings.Contains(r.Payload, "failure") {
			return &compliance.ReleaseResponse{HasError: true, Error: "PRIVATE"}, nil
		}
		return &compliance.ReleaseResponse{Payload: `{"shared":"clear","global":"clear"}`}, nil
	}}, model.Descriptor())
	reader := readmodels.For(service, model)
	result, err := reader.ReleaseMany(ctx, []IndependentSecrets{{Shared: "ciphertext"}, {Global: "ciphertext"}})
	if err != nil || len(result) != 2 || result[0].Shared != "clear" || result[1].Global != "clear" {
		t.Fatalf("collection release failed: %v", err)
	}
	result, err = reader.ReleaseMany(ctx, []IndependentSecrets{{Shared: "ciphertext"}, {Global: "failure"}})
	if result != nil || !errors.Is(err, readmodels.ErrRelease) || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("partial release escaped: %v", err)
	}
}

func TestMixedScopesStillRequireOwnerForSubjectValues(t *testing.T) {
	type Mixed struct {
		Name   string `json:"name" chronicle:"pii"`
		Secret string `json:"secret" chronicle:"encrypted(scope=namespace)"`
	}
	model, err := readmodels.Define[Mixed]()
	if err != nil {
		t.Fatal(err)
	}
	service, ctx := serviceFixture(t, &modelKernel{release: func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		t.Error("missing owner dispatched")
		return &compliance.ReleaseResponse{Payload: `{}`}, nil
	}}, model.Descriptor())
	if value, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"name":"ciphertext","secret":"ciphertext"}`)); value != nil || !errors.Is(err, readmodels.ErrRelease) {
		t.Fatal("missing subject did not fail closed")
	}
}

func TestTaggedSubjectFallsBackToIDButNotProjectionKey(t *testing.T) {
	type Model struct {
		Key   string  `json:"key" chronicle:"key"`
		ID    string  `json:"identity"`
		Owner *string `json:"owner" chronicle:"subject"`
		Name  string  `json:"name" chronicle:"pii"`
	}
	model, err := readmodels.Define[Model]()
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan string, 8)
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		requests <- r.Subject
		return &compliance.ReleaseResponse{Payload: r.Payload}, nil
	}}, model.Descriptor())
	for _, owner := range []string{`null`, `""`, `"explicit"`, `"00000000-0000-0000-0000-000000000000"`} {
		_, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"identity":"fallback","key":"not-owner","owner":`+owner+`,"name":"ciphertext"}`))
		if err != nil {
			t.Fatal(err)
		}
		want := strings.Trim(owner, `"`)
		if owner == `null` || owner == `""` {
			want = "fallback"
		}
		if got := <-requests; got != want {
			t.Fatalf("subject = %s, want %s", got, want)
		}
	}
	if value, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"key":"not-owner","name":"ciphertext"}`)); value != nil || !errors.Is(err, readmodels.ErrRelease) {
		t.Fatal("projection key used as owner")
	}
}

func TestAllOneShotReadPathsFailClosedOnRelease(t *testing.T) {
	model := person(t, readmodels.WithPII("name"), readmodels.WithObserver(readmodels.Projection, "people"))
	for _, path := range []string{"get", "session", "replay"} {
		t.Run(path, func(t *testing.T) {
			service, ctx := serviceFixture(t, &modelKernel{
				get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
					return &contracts.GetInstanceByKeyResponse{ReadModel: `{"id":"owner","name":"ciphertext"}`, LastHandledEventSequenceNumber: uint64(events.Unavailable)}, nil
				},
				replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
					// Replay is released by the kernel, not Compliance.Release on the client.
					return nil, errors.New("PRIVATE kernel release failed")
				},
				release: func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
					return &compliance.ReleaseResponse{HasError: true, Error: "PRIVATE"}, nil
				},
				dehydrate: func(context.Context, *contracts.DehydrateSessionRequest) (*emptypb.Empty, error) {
					return &emptypb.Empty{}, nil
				},
			}, model.Descriptor())
			reader := readmodels.For(service, model)
			var err error
			switch path {
			case "get":
				value, failure := reader.Get(ctx, "owner")
				err = failure
				if value.Exists || value.Value.Name != "" {
					t.Fatal("unsafe get")
				}
			case "session":
				session, failure := reader.NewSession("owner")
				if failure != nil {
					t.Fatal(failure)
				}
				value, failure := session.Get(ctx)
				err = failure
				if value.Exists || value.Value.Name != "" {
					t.Fatal("unsafe session")
				}
				if failure := session.Close(ctx); failure != nil {
					t.Fatal(failure)
				}
			case "replay":
				value, failure := service.ReplayProjection(ctx, model.Identifier(), 1)
				err = failure
				if value != nil {
					t.Fatal("unsafe replay")
				}
			}
			if err == nil || (path != "replay" && !errors.Is(err, readmodels.ErrRelease)) || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("release identity: %v", err)
			}
		})
	}
}

func TestReleasePreservesStoredLineageAgainstUnexpectedReplyFields(t *testing.T) {
	model := person(t, readmodels.WithPII("name"))
	service, ctx := serviceFixture(t, &modelKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		return &compliance.ReleaseResponse{Payload: `{"name":"clear","__subject":"wrong","__subjects":{"name":"wrong"},"extra":"PRIVATE"}`}, nil
	}}, model.Descriptor())
	value, err := service.Release(ctx, model.Identifier(), json.RawMessage(`{"id":"owner","name":"ciphertext","__subject":"owner","__subjects":{"name":"owner"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(value), "wrong") || strings.Contains(string(value), "PRIVATE") {
		t.Fatal("unowned fields merged")
	}
}
