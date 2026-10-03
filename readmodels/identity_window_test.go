// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/base64"
	"errors"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	compliancecontracts "github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc"
)

type lowercaseIdentityWindow struct {
	Value string `json:"id"`
}
type uppercaseIdentityWindow struct {
	Value string `json:"ID"`
}
type pascalIdentityWindow struct {
	Value string `json:"Id"`
}
type mixedIdentityWindow struct {
	Value string `json:"iD"`
}
type mongoIdentityWindow struct {
	Value string `json:"_id"`
}
type defaultIdentityWindow struct {
	ID string
}

func refuseIdentityWindows[T any](t *testing.T, policy serialization.NamingPolicy, options ...readmodels.ModelOption) {
	t.Helper()
	// Declaration remains legal: a string ID is not an event-source identity.
	model, err := readmodels.Define[T](options...)
	if err != nil {
		t.Fatal("declaration rejected", err)
	}
	descriptor, err := model.Descriptor().WithNamingPolicy(policy)
	if err != nil {
		t.Fatal(err)
	}
	var rpcs atomic.Int32
	kernel := &watchKernel{
		get: func(context.Context, *contracts.GetInstancesRequest) (*contracts.GetInstancesResponse, error) {
			rpcs.Add(1)
			return nil, errors.New("unexpected GetInstances")
		},
		observe: func(*contracts.ObserveInstancesRequest, grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error {
			rpcs.Add(1)
			return errors.New("unexpected ObserveInstances")
		},
		release: func(context.Context, *compliancecontracts.ReleaseRequest) (*compliancecontracts.ReleaseResponse, error) {
			rpcs.Add(1)
			return nil, errors.New("unexpected Release")
		},
	}
	service, ctx := watchFixture(t, kernel, descriptor)
	reader := readmodels.For(service, model).Materialized()
	if values, err := service.Materialized().GetInstances(ctx, model.Identifier(), nil); values != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Errorf("raw GetInstances = %v, %v; want nil, ErrUnsupported", values, err)
	}
	if values, err := reader.GetInstances(ctx, nil); values != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Errorf("typed GetInstances = %v, %v; want nil, ErrUnsupported", values, err)
	}
	if sub, err := service.Materialized().ObserveInstances(ctx, model.Identifier(), nil); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Errorf("raw ObserveInstances = %v, %v; want nil, ErrUnsupported", sub, err)
	}
	if sub, err := reader.ObserveInstances(ctx, nil); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Errorf("typed ObserveInstances = %v, %v; want nil, ErrUnsupported", sub, err)
	}
	if got := rpcs.Load(); got != 0 {
		t.Errorf("server RPCs including Release = %d, want zero", got)
	}
}

func TestClassifiedMongoIdentityWindowsRefuseBeforeRPC(t *testing.T) {
	aliases := []struct {
		name string
		run  func(*testing.T, serialization.NamingPolicy, ...readmodels.ModelOption)
	}{
		{"id", refuseIdentityWindows[lowercaseIdentityWindow]},
		{"ID", refuseIdentityWindows[uppercaseIdentityWindow]},
		{"Id", refuseIdentityWindows[pascalIdentityWindow]},
		{"iD", refuseIdentityWindows[mixedIdentityWindow]},
		{"_id", refuseIdentityWindows[mongoIdentityWindow]},
	}
	for _, classification := range []struct {
		name     string
		metadata compliance.Classification
	}{
		{"PII", compliance.Classification{PII: true}},
		{"EncryptedNamespace", compliance.Classification{Encrypted: true, Scope: compliance.Namespace}},
	} {
		for _, alias := range aliases {
			t.Run(classification.name+"/"+alias.name, func(t *testing.T) {
				alias.run(t, serialization.PreservePropertyNames, readmodels.WithProtection(compliance.Property(alias.name, classification.metadata)))
			})
		}
		for _, policy := range []struct {
			name   string
			policy serialization.NamingPolicy
		}{
			{"preserve", serialization.PreservePropertyNames},
			{"camelCase", serialization.CamelCase},
			{"legacyGoCamelCase", serialization.LegacyGoCamelCase},
		} {
			t.Run(classification.name+"/default-ID/"+policy.name, func(t *testing.T) {
				refuseIdentityWindows[defaultIdentityWindow](t, policy.policy, readmodels.WithProtection(compliance.Property("Id", classification.metadata)))
			})
		}
	}
}

func TestFrozenProviderAndTypeClassifiedIdentityWindowsRefuse(t *testing.T) {
	for _, metadata := range []compliance.Classification{{PII: true}, {Encrypted: true, Scope: compliance.Namespace}} {
		t.Run("provider", func(t *testing.T) {
			calls := 0
			provider := compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
				calls++
				if target.Field == "ID" {
					return metadata, nil
				}
				return compliance.Classification{}, nil
			})
			// The provider may run during Define only, not rebinding or admission.
			model, err := readmodels.Define[defaultIdentityWindow](readmodels.WithProtection(provider))
			if err != nil {
				t.Fatal(err)
			}
			frozen := calls
			descriptor, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
			if err != nil {
				t.Fatal(err)
			}
			service, ctx := watchFixture(t, &watchKernel{}, descriptor)
			reader := readmodels.For(service, model).Materialized()
			if values, err := service.Materialized().GetInstances(ctx, model.Identifier(), nil); values != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("raw provider window admitted", err)
			}
			if values, err := reader.GetInstances(ctx, nil); values != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("typed provider window admitted", err)
			}
			if sub, err := service.Materialized().ObserveInstances(ctx, model.Identifier(), nil); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("raw provider watch admitted", err)
			}
			if sub, err := reader.ObserveInstances(ctx, nil); sub != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("typed provider watch admitted", err)
			}
			if calls != frozen {
				t.Fatal("classification provider reran")
			}
		})
		t.Run("type", func(t *testing.T) {
			refuseIdentityWindows[defaultIdentityWindow](t, serialization.PreservePropertyNames, readmodels.WithProtection(compliance.For[defaultIdentityWindow](metadata)))
		})
	}
}

type ordinaryProtectedWindow struct {
	ID    string `json:"name"`
	Upper string `json:"_ID"`
}

func TestMaterializedProfileUsesSerializedIdentityNamesNotGoFields(t *testing.T) {
	for _, metadata := range []compliance.Classification{{PII: true}, {Encrypted: true, Scope: compliance.Namespace}} {
		// MongoDB's exact _id alias does not include _ID. A Go ID explicitly
		// named "name" is not a MongoDB key either. Both remain ordinary roots.
		model, err := readmodels.Define[ordinaryProtectedWindow](readmodels.WithProtection(
			compliance.Property("name", metadata), compliance.Property("_ID", metadata)))
		if err != nil {
			t.Fatal(err)
		}
		plaintext := base64.StdEncoding.EncodeToString(make([]byte, 256))
		document := `{"name":"` + plaintext + `","_ID":"plain"}`
		var releases atomic.Int32
		kernel := &watchKernel{
			get: func(context.Context, *contracts.GetInstancesRequest) (*contracts.GetInstancesResponse, error) {
				return &contracts.GetInstancesResponse{Instances: []string{document}}, nil
			},
			observe: func(_ *contracts.ObserveInstancesRequest, stream grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error {
				if err := stream.Send(&contracts.ObserveInstancesResponse{Instances: []string{document}}); err != nil {
					return err
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			},
			release: func(context.Context, *compliancecontracts.ReleaseRequest) (*compliancecontracts.ReleaseResponse, error) {
				releases.Add(1)
				return nil, errors.New("unexpected second release")
			},
		}
		service, ctx := watchFixture(t, kernel, model.Descriptor())
		reader := readmodels.For(service, model).Materialized()
		if values, err := service.Materialized().GetInstances(ctx, model.Identifier(), nil); err != nil || len(values) != 1 || string(values[0]) != document {
			t.Fatal("raw ordinary protected window changed", err)
		}
		if values, err := reader.GetInstances(ctx, nil); err != nil || len(values) != 1 || values[0].ID != plaintext || values[0].Upper != "plain" {
			t.Fatal("typed ordinary protected window changed", err)
		}
		rawSub, err := service.Materialized().ObserveInstances(ctx, model.Identifier(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if values, err := rawSub.Recv(); err != nil || len(values) != 1 || string(values[0]) != document {
			t.Fatal("raw ordinary protected snapshot changed", err)
		}
		if err := rawSub.Close(); err != nil {
			t.Fatal(err)
		}
		typedSub, err := reader.ObserveInstances(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if values, err := typedSub.Recv(); err != nil || len(values) != 1 || values[0].ID != plaintext || values[0].Upper != "plain" {
			t.Fatal("typed ordinary protected snapshot changed", err)
		}
		if err := typedSub.Close(); err != nil {
			t.Fatal(err)
		}
		if releases.Load() != 0 {
			t.Fatal("server-released plaintext sent to Release")
		}
	}
}
