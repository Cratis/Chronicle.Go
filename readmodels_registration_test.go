// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Person struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type readModelKernel struct {
	contracts.UnimplementedReadModelsServer
	register func(context.Context, *contracts.RegisterManyRequest) error
	get      func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error)
}

func (k *readModelKernel) RegisterMany(ctx context.Context, r *contracts.RegisterManyRequest) (*emptypb.Empty, error) {
	if err := k.register(ctx, r); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}
func (k *readModelKernel) GetInstanceByKey(ctx context.Context, r *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
	return k.get(ctx, r)
}

func modelRegistry(t *testing.T) (*Registry, readmodels.Model[Person]) {
	t.Helper()
	registry := NewRegistry()
	if _, err := RegisterEvent[lifecycleEvent](registry); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[Person](registry, readmodels.WithIdentifier("Example.Person"), readmodels.WithIndexes("name"))
	if err != nil {
		t.Fatal(err)
	}
	return registry, model
}

func TestReadModelRegistrationMatchesCSharpGolden(t *testing.T) {
	registry, _ := modelRegistry(t)
	requests := make(chan *contracts.RegisterManyRequest, 1)
	kernel := &supervisedKernel{readModels: &readModelKernel{register: func(_ context.Context, r *contracts.RegisterManyRequest) error { requests <- r; return nil }}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := store.WaitForRegistration(ctx)
	if err != nil || len(outcome.Artifacts) != 5 || outcome.Artifacts[3].Name != "read-models" || outcome.Artifacts[4].Name != "constraints" {
		// C# EventStore.RegisterAllArtifacts registers read models with event types, before constraints.
		t.Fatalf("%+v %v", outcome, err)
	}
	data, err := os.ReadFile("testdata/readmodels/registration.json")
	if err != nil {
		t.Fatal(err)
	}
	expected := &contracts.RegisterManyRequest{}
	if err = protojson.Unmarshal(data, expected); err != nil {
		t.Fatal(err)
	}
	actual := <-requests
	if !proto.Equal(expected, actual) {
		t.Fatalf("registration:\n%s\nwant:\n%s", protojson.Format(actual), protojson.Format(expected))
	}
}

func TestReadModelReplayBarrierAndNamespaceIsolation(t *testing.T) {
	registry, model := modelRegistry(t)
	var registrations, reads atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	kernel := &supervisedKernel{}
	kernel.readModels = &readModelKernel{
		register: func(ctx context.Context, _ *contracts.RegisterManyRequest) error {
			pass := registrations.Add(1)
			if kernel.registrations.Load() != pass {
				t.Error("read models registered before event types")
			}
			if pass == 2 {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
		get: func(_ context.Context, r *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
			reads.Add(1)
			return &contracts.GetInstanceByKeyResponse{ReadModel: `{"id":"` + r.Namespace + `"}`, LastHandledEventSequenceNumber: 0}, nil
		},
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	one, err := client.EventStore(ctx, "store", WithNamespace("one"))
	if err != nil {
		t.Fatal(err)
	}
	two, err := client.EventStore(ctx, "store", WithNamespace("two"))
	if err != nil {
		t.Fatal(err)
	}
	if registrations.Load() != 1 {
		t.Fatal("definitions registered twice for namespaces")
	}
	before, err := one.WaitForRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	kernel.endStream <- status.Error(codes.Unavailable, "lost")
	awaitSignal(t, ctx, entered)
	bounded, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	_, err = readmodels.For(one.ReadModels(), model).Get(bounded, "key")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || reads.Load() != 0 {
		t.Fatalf("read bypassed barrier: %v", err)
	}
	close(release)
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	for _, store := range []*EventStore{one, two} {
		result, err := readmodels.For(store.ReadModels(), model).Get(ctx, "key")
		if err != nil || !result.Exists || result.Value.ID != string(store.Namespace()) {
			t.Fatalf("namespace read: %+v %v", result, err)
		}
	}
	after, err := two.WaitForRegistration(ctx)
	if err != nil || after.Generation <= before.Generation || registrations.Load() != 2 {
		t.Fatalf("replay: %+v %v", after, err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = readmodels.For(one.ReadModels(), model).Get(ctx, "key"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestReadModelRegistrationFailureDoesNotPublishReadiness(t *testing.T) {
	registry, _ := modelRegistry(t)
	kernel := &supervisedKernel{readModels: &readModelKernel{register: func(context.Context, *contracts.RegisterManyRequest) error {
		return status.Error(codes.Unimplemented, "old kernel")
	}}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	store, err := client.EventStore(ctx, "store")
	var registration *RegistrationError
	if store != nil || !errors.Is(err, ErrUnsupported) || !errors.As(err, &registration) || registration.Outcome.IsSuccess() || registration.Outcome.Artifacts[3].Failure == nil {
		t.Fatalf("%v %v", store, err)
	}
}

func TestReadModelRegistrySnapshotsAndConcurrentDuplicates(t *testing.T) {
	registry := NewRegistry()
	var accepted atomic.Int32
	var workers sync.WaitGroup
	for range 12 {
		workers.Go(func() {
			_, err := RegisterReadModel[Person](registry)
			if err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, ErrInvalidConfiguration) {
				t.Error(err)
			}
		})
	}
	workers.Wait()
	if accepted.Load() != 1 {
		t.Fatal(accepted.Load())
	}
	selected := NewRegistry()
	if _, err := RegisterReadModel[Person](selected, readmodels.WithIdentifier("selected")); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry), WithRegistryForStore("other", selected))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	type Later struct{ Value string }
	if _, err = RegisterReadModel[Later](registry); err != nil {
		t.Fatal(err)
	}
	if len(client.readModelCatalog.Descriptors()) != 1 || len(client.readModelCatalogs["other"].Descriptors()) != 1 {
		t.Fatal("snapshot mutated")
	}
	if _, ok := client.readModelCatalogs["other"].LookupIdentifier("selected"); !ok {
		t.Fatal("store-specific catalog lost")
	}
	if _, err = RegisterReadModel[Later](nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}
