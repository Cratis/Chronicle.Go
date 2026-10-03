// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	compliancecontracts "github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
)

func TestMaterializedWindowsNeverDecryptReleasedPlaintextAgain(t *testing.T) {
	plaintext := base64.StdEncoding.EncodeToString(make([]byte, 256))
	model := person(t, readmodels.WithObserver(readmodels.Projection, "people"), readmodels.WithPII("name"))
	for _, raw := range []bool{false, true} {
		for _, invalid := range []string{`{"id":"person","name":42}`, `{"id":"person"}`, `{"id":"person","name":null}`} {
			k := &watchKernel{release: func(context.Context, *compliancecontracts.ReleaseRequest) (*compliancecontracts.ReleaseResponse, error) {
				t.Error("second decryption RPC")
				return nil, nil
			}}
			k.get = func(context.Context, *contracts.GetInstancesRequest) (*contracts.GetInstancesResponse, error) {
				return &contracts.GetInstancesResponse{Instances: []string{`{"id":"person","name":"` + plaintext + `"}`}}, nil
			}
			k.observe = func(_ *contracts.ObserveInstancesRequest, stream grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error {
				for _, value := range []string{`{"id":"person","name":"` + plaintext + `"}`, invalid} {
					if err := stream.Send(&contracts.ObserveInstancesResponse{Instances: []string{value}}); err != nil {
						return err
					}
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			}
			s, ctx := watchFixture(t, k, model.Descriptor())
			reader := readmodels.For(s, model)
			if raw {
				page, err := s.Materialized().GetInstances(ctx, model.Identifier(), nil)
				if err != nil || len(page) != 1 {
					t.Fatal("page", err)
				}
				var value Person
				if err := json.Unmarshal(page[0], &value); err != nil || value.Name != plaintext {
					t.Fatal("raw plaintext changed", err)
				}
				sub, err := s.Materialized().ObserveInstances(ctx, model.Identifier(), nil)
				if err != nil {
					t.Fatal(err)
				}
				values, err := sub.Recv()
				if err != nil || len(values) != 1 {
					t.Fatal("first window", err)
				}
				if err := json.Unmarshal(values[0], &value); err != nil || value.Name != plaintext {
					t.Fatal("raw plaintext changed", err)
				}
				values, err = sub.Recv()
				if !errors.Is(err, readmodels.ErrRelease) || values != nil {
					t.Fatal("invalid raw window emitted", err)
				}
				if err := sub.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				page, err := reader.Materialized().GetInstances(ctx, nil)
				if err != nil || len(page) != 1 || page[0].Name != plaintext {
					t.Fatal("typed plaintext changed", err)
				}
				sub, err := reader.Materialized().ObserveInstances(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				values, err := sub.Recv()
				if err != nil || len(values) != 1 || values[0].Name != plaintext {
					t.Fatal("typed plaintext changed", err)
				}
				values, err = sub.Recv()
				if !errors.Is(err, readmodels.ErrRelease) || values != nil {
					t.Fatal("invalid typed window emitted", err)
				}
				if err := sub.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

func TestClassifiedWatchesRefuseBeforeTransportOrLocalAttachment(t *testing.T) {
	for _, kind := range []readmodels.ObserverType{readmodels.Projection, readmodels.Reducer} {
		for _, classification := range []compliance.Classification{{PII: true}, {Encrypted: true}, {Encrypted: true, Scope: compliance.Namespace}, {Encrypted: true, Scope: compliance.Global}} {
			calls := 0
			model := person(t, readmodels.WithObserver(kind, "people"), readmodels.WithProtection(compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
				calls++
				if target.Field == "Name" {
					return classification, nil
				}
				return compliance.Classification{}, nil
			})))
			frozen := calls
			// No server methods or local generation are installed. Admission must
			// return ErrUnsupported, not open an RPC or report ErrInterrupted.
			s, ctx := watchFixture(t, &watchKernel{}, model.Descriptor(), readmodels.WithReductionChanges(&readmodels.ReductionChanges{}))
			if sub, err := s.Watch(ctx, model.Identifier()); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("raw classified watch admitted", err)
			}
			if sub, err := readmodels.For(s, model).Watch(ctx); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("typed classified watch admitted", err)
			}
			if calls != frozen {
				t.Fatal("provider reran during admission")
			}
		}
	}
}

func TestUnwitnessedMaterializedProfilesRefuseBeforeRPC(t *testing.T) {
	type nested struct {
		Name string `chronicle:"pii"`
	}
	type modelType struct {
		ID    string
		Value nested
	}
	for _, options := range [][]readmodels.ModelOption{
		{readmodels.WithProtection(compliance.For[nested](compliance.Classification{PII: true}))},
		nil,
	} {
		model, err := readmodels.Define[modelType](options...)
		if err != nil {
			t.Fatal(err)
		}
		s, ctx := watchFixture(t, &watchKernel{}, model.Descriptor())
		if values, err := s.Materialized().GetInstances(ctx, model.Identifier(), nil); values != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("nested window admitted", err)
		}
		if sub, err := readmodels.For(s, model).Materialized().ObserveInstances(ctx, nil); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("nested window admitted", err)
		}
	}
	for _, option := range []readmodels.ModelOption{
		readmodels.WithProtection(compliance.Property("name", compliance.Classification{Encrypted: true})),
		readmodels.WithProtection(compliance.Property("name", compliance.Classification{Encrypted: true, Scope: compliance.Global})),
		readmodels.WithSink(readmodels.Sink{Type: readmodels.SQL}),
	} {
		// Scope cases use only encryption; the sink case uses PII.
		model := person(t, option)
		if model.Descriptor().Sink().Type == readmodels.SQL {
			model = person(t, option, readmodels.WithPII("name"))
		}
		s, ctx := watchFixture(t, &watchKernel{}, model.Descriptor())
		if values, err := readmodels.For(s, model).Materialized().GetInstances(ctx, nil); values != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("unwitnessed window admitted", err)
		}
		if sub, err := s.Materialized().ObserveInstances(ctx, model.Identifier(), nil); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("unwitnessed window admitted", err)
		}
	}
}

func TestLocalReducerWatchPlaintextRemovalAndGenerationIsolation(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Reducer, "people"))
	source := &readmodels.ReductionChanges{}
	k := &watchKernel{release: func(context.Context, *compliancecontracts.ReleaseRequest) (*compliancecontracts.ReleaseResponse, error) {
		t.Error("local plaintext sent to release")
		return nil, nil
	}}
	s, ctx := watchFixture(t, k, model.Descriptor(), readmodels.WithReductionChanges(source))
	generation, cancel := context.WithCancel(ctx)
	defer cancel()
	source.BindGeneration(generation)
	sub, err := readmodels.For(s, model).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Error(err)
		}
	}()
	source.Publish(generation, model.Identifier(), "person", json.RawMessage(`{"id":"person","name":"Ada"}`))
	change, err := sub.Recv()
	if err != nil || change.Type != readmodels.Modified || change.Value.Name != "Ada" {
		t.Fatalf("local: %+v %v", change, err)
	}
	source.Publish(generation, model.Identifier(), "person", nil)
	change, err = sub.Recv()
	if err != nil || change.Type != readmodels.Removed || change.HasValue {
		t.Fatalf("removal: %+v %v", change, err)
	}
	cancel()
	<-sub.Done()
	if !errors.Is(sub.Err(), readmodels.ErrInterrupted) {
		t.Fatal(sub.Err())
	}
	nextGeneration, nextCancel := context.WithCancel(ctx)
	defer nextCancel()
	source.BindGeneration(nextGeneration)
	next, err := readmodels.For(s, model).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := next.Close(); err != nil {
			t.Error(err)
		}
	}()
	source.Publish(generation, model.Identifier(), "old", json.RawMessage(`{"id":"old"}`))
	source.Publish(nextGeneration, model.Identifier(), "new", json.RawMessage(`{"id":"new"}`))
	change, err = next.Recv()
	if err != nil || change.Key != "new" {
		t.Fatalf("retired generation leaked: %+v %v", change, err)
	}
}
