// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func TestConfidentialityWatchesAndWindowsRefuseBeforeRPCWithoutSubject(t *testing.T) {
	model, err := readmodels.Define[IndependentSecrets](readmodels.WithObserver(readmodels.Projection, "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	var allRPCs, watches, observations, releases atomic.Int32
	unexpectedRPC := errors.New("unexpected RPC for unsupported protection profile")
	conn, err := grpc.NewClient("passthrough:///unsupported-profile",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(_ context.Context, method string, _, _ any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption) error {
			allRPCs.Add(1)
			if method == compliance.Compliance_Release_FullMethodName {
				releases.Add(1)
			}
			return unexpectedRPC
		}),
		grpc.WithStreamInterceptor(func(_ context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, method string, _ grpc.Streamer, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			allRPCs.Add(1)
			switch method {
			case contracts.ReadModels_Watch_FullMethodName:
				watches.Add(1)
			case contracts.MaterializedReadModels_ObserveInstances_FullMethodName:
				observations.Add(1)
			}
			return nil, unexpectedRPC
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		if allRPCs.Load() != 0 || watches.Load() != 0 || observations.Load() != 0 || releases.Load() != 0 {
			t.Errorf("RPCs: all=%d Watch=%d ObserveInstances=%d Release=%d; want zero", allRPCs.Load(), watches.Load(), observations.Load(), releases.Load())
		}
	})
	service, err := readmodels.New("store", "tenant-a", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	reader := readmodels.For(service, model)
	ctx := t.Context()
	// IndependentSecrets mixes namespace and global confidentiality. All
	// classified watches and this unsupported mixed/global window profile must
	// refuse before transport. No ciphertext or fabricated clear reply is needed.
	t.Run("raw watch", func(t *testing.T) {
		if sub, err := service.Watch(ctx, model.Identifier()); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("classified watch was not refused before RPC", err)
		}
	})
	t.Run("typed watch", func(t *testing.T) {
		if sub, err := reader.Watch(ctx); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("classified watch was not refused before RPC", err)
		}
	})
	t.Run("raw window", func(t *testing.T) {
		if values, err := service.Materialized().GetInstances(ctx, model.Identifier(), nil); values != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("mixed/global window was not refused before RPC", err)
		}
		if sub, err := service.Materialized().ObserveInstances(ctx, model.Identifier(), nil); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("mixed/global window was not refused before RPC", err)
		}
	})
	t.Run("typed window", func(t *testing.T) {
		if values, err := reader.Materialized().GetInstances(ctx, nil); values != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("mixed/global window was not refused before RPC", err)
		}
		if sub, err := reader.Materialized().ObserveInstances(ctx, nil); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("mixed/global window was not refused before RPC", err)
		}
	})
}
