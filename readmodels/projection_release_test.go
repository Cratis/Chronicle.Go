// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	compliancecontracts "github.com/cratis/chronicle.go/contracts/compliance"
	explorer "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

type replayProtectedNode struct {
	Value string
	Next  *replayProtectedNode
}
type replayProtectedModel struct {
	ID   string
	Node replayProtectedNode
}

func TestProtectedProjectionRoutesRefuseBeforeRPCRegardlessOfIdentityAndClassification(t *testing.T) {
	for _, classification := range []compliance.Classification{{PII: true}, {Encrypted: true}, {Encrypted: true, Scope: compliance.Namespace}, {Encrypted: true, Scope: compliance.Global}} {
		for _, passive := range []bool{false, true} {
			providerCalls := 0
			options := []readmodels.ModelOption{readmodels.WithObserver(readmodels.Projection, "projection"), readmodels.WithProtection(compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
				providerCalls++
				if target.Field == "Value" {
					return classification, nil
				}
				return compliance.Classification{}, nil
			}))}
			if passive {
				options = append(options, readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink}))
			}
			model, err := readmodels.Define[replayProtectedModel](options...)
			if err != nil {
				t.Fatal(err)
			}
			frozenCalls := providerCalls
			rpcs := 0
			kernel := &modelKernel{options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)},
				replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
					rpcs++
					return nil, nil
				},
				snapshots: func(context.Context, *explorer.AllSnapshotsForReadModelRequest) (*explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
					rpcs++
					return nil, nil
				},
				get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
					rpcs++
					return nil, nil
				},
				release: func(context.Context, *compliancecontracts.ReleaseRequest) (*compliancecontracts.ReleaseResponse, error) {
					rpcs++
					return nil, nil
				},
			}
			service, ctx := serviceFixture(t, kernel, model.Descriptor())
			reader := readmodels.For(service, model)
			if v, err := reader.GetAll(ctx, new(events.Count(1))); v.Instances != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("protected replay admitted", err)
			}
			if v, err := reader.GetSnapshots(ctx, "source-not-subject"); v != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("protected history admitted", err)
			}
			if v, err := service.ReplayProjection(ctx, model.Identifier(), 1); v != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("legacy replay admitted", err)
			}
			if v, err := reader.NewSession("source-not-subject"); v != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatal("protected session admitted", err)
			}
			if passive {
				if v, err := reader.GetAll(ctx, nil); v.Instances != nil || !errors.Is(err, chronicle.ErrUnsupported) {
					t.Fatal("passive replay admitted", err)
				}
				if v, err := reader.Get(ctx, "source-not-subject"); v.Exists || !errors.Is(err, chronicle.ErrUnsupported) {
					t.Fatal("immediate replay admitted", err)
				}
			}
			if v, err := reader.GetAll(ctx, new(events.Count(0))); err != nil || len(v.Instances) != 0 || v.Instances == nil {
				t.Fatal("zero refusal", err)
			}
			if rpcs != 0 || providerCalls != frozenCalls {
				t.Fatal("admission performed I/O or reran provider")
			}
		}
	}
}

func TestProtectedMaterializedGetNeverReleasesPlaintextAgain(t *testing.T) {
	model := person(t, readmodels.WithPII("name"))
	// String validation cannot distinguish this from encrypted content. Route
	// ownership, not a ciphertext heuristic, makes the second release forbidden.
	const plaintext = "Q0VOVgEAAAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxw="
	service, ctx := serviceFixture(t, &modelKernel{
		get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
			return &contracts.GetInstanceByKeyResponse{ReadModel: `{"id":"source","name":"` + plaintext + `"}`}, nil
		},
		release: func(context.Context, *compliancecontracts.ReleaseRequest) (*compliancecontracts.ReleaseResponse, error) {
			t.Error("second decryption RPC")
			return nil, nil
		},
	}, model.Descriptor())
	value, err := readmodels.For(service, model).Get(ctx, "source")
	if err != nil || !value.Exists || value.Value.Name != plaintext {
		t.Fatal("plaintext changed", err)
	}
}
