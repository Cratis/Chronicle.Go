// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	compliancecontracts "github.com/cratis/chronicle.go/contracts/compliance"
	explorer "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/types/known/emptypb"
)

type replayProtectedNode struct {
	Value string
	Next  *replayProtectedNode
}
type replayProtectedModel struct {
	ID   string
	Node replayProtectedNode
}

// Chronicle#4561 is fixed in 19.32.2: every projection replay route returns
// values the kernel already released with their original subjects. The SDK
// admits classified models on these routes and never decrypts them again.
func TestProtectedProjectionRoutesUseKernelReleaseWithoutSecondDecryption(t *testing.T) {
	// String validation cannot distinguish this from encrypted content; a second
	// release would corrupt legitimate plaintext.
	const plaintext = "Q0VOVgEAAAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxw="
	document := `{"ID":"source-not-subject","Node":{"Value":"` + plaintext + `","Next":null}}`
	for _, classification := range []compliance.Classification{{PII: true}, {Encrypted: true}, {Encrypted: true, Scope: compliance.Namespace}, {Encrypted: true, Scope: compliance.Global}} {
		for _, passive := range []bool{false, true} {
			options := []readmodels.ModelOption{readmodels.WithObserver(readmodels.Projection, "projection"), readmodels.WithProtection(compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
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
			kernel := &modelKernel{options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)},
				replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
					return &contracts.GetAllInstancesResponse{Instances: []string{document}, ProcessedEventsCount: 1}, nil
				},
				snapshots: func(context.Context, *explorer.AllSnapshotsForReadModelRequest) (*explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
					return &explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*explorer.ReadModelSnapshotResponse{{Instance: document, Occurred: historyTime()}}}, nil
				},
				get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
					return &contracts.GetInstanceByKeyResponse{ReadModel: document, LastHandledEventSequenceNumber: 1}, nil
				},
				dehydrate: func(context.Context, *contracts.DehydrateSessionRequest) (*emptypb.Empty, error) {
					return &emptypb.Empty{}, nil
				},
				release: func(context.Context, *compliancecontracts.ReleaseRequest) (*compliancecontracts.ReleaseResponse, error) {
					t.Error("second decryption RPC")
					return nil, nil
				},
			}
			service, ctx := serviceFixture(t, kernel, model.Descriptor())
			reader := readmodels.For(service, model)
			if v, err := reader.GetAll(ctx, new(events.Count(1))); err != nil || len(v.Instances) != 1 || v.Instances[0].Value.Node.Value != plaintext {
				t.Fatal("protected replay", err)
			}
			if v, err := reader.GetSnapshots(ctx, "source-not-subject"); err != nil || len(v) != 1 || v[0].Instance.Node.Value != plaintext {
				t.Fatal("protected history", err)
			}
			if v, err := service.ReplayProjection(ctx, model.Identifier(), 1); err != nil || len(v) != 1 {
				t.Fatal("legacy replay", err)
			}
			session, err := reader.NewSession("source-not-subject")
			if err != nil {
				t.Fatal("protected session", err)
			}
			hydrated, err := session.Get(ctx)
			if closeErr := session.Close(ctx); closeErr != nil {
				t.Fatal(closeErr)
			}
			if err != nil || !hydrated.Exists || hydrated.Value.Node.Value != plaintext {
				t.Fatal("protected session read", err)
			}
			if passive {
				if v, err := reader.GetAll(ctx, nil); err != nil || len(v.Instances) != 1 || v.Instances[0].Value.Node.Value != plaintext {
					t.Fatal("passive replay", err)
				}
				if v, err := reader.Get(ctx, "source-not-subject"); err != nil || !v.Exists || v.Value.Node.Value != plaintext {
					t.Fatal("immediate replay", err)
				}
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
