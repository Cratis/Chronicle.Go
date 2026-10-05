// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	explorer "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
)

// Tests are deliberately serial: this concept supplies a per-test application
// callback to prove the production counted transport lease is no longer held.
var historyDecodeHook func() error

type historyCodec string

func (v historyCodec) ConceptValue() string             { return string(v) }
func (v historyCodec) MarshalJSON() ([]byte, error)     { return json.Marshal(string(v)) }
func (v historyCodec) MarshalText() ([]byte, error)     { return []byte(v), nil }
func (v *historyCodec) UnmarshalText(data []byte) error { *v = historyCodec(data); return nil }
func (v *historyCodec) UnmarshalJSON(data []byte) error {
	if historyDecodeHook != nil {
		if err := historyDecodeHook(); err != nil {
			return err
		}
	}
	return json.Unmarshal(data, (*string)(v))
}

type historyCodecModel struct {
	Value historyCodec `json:"value"`
}
type historyLifetimeKernel struct {
	readModelKernel
	explorer.UnimplementedReadModelExplorerServer
}

func (*historyLifetimeKernel) GetAllInstances(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
	return &contracts.GetAllInstancesResponse{Instances: []string{`{"value":"one"}`, `{"value":"two"}`}, ProcessedEventsCount: 2}, nil
}
func (*historyLifetimeKernel) AllSnapshotsForReadModel(context.Context, *explorer.AllSnapshotsForReadModelRequest) (*explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
	return &explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*explorer.ReadModelSnapshotResponse{
		{Instance: `{"value":"one"}`, Occurred: &explorer.SerializableDateTimeOffset{Value: "2026-01-01T00:00:00Z"}},
		{Instance: `{"value":"two"}`, Occurred: &explorer.SerializableDateTimeOffset{Value: "2026-01-02T00:00:00Z"}},
	}}, nil
}

func TestModelHistoryCodecsCanCloseClientAndLateCancellationDiscardsEverything(t *testing.T) {
	for _, snapshots := range []bool{false, true} {
		for _, cancelDuringDecode := range []bool{false, true} {
			r := NewRegistry()
			model, err := RegisterReadModel[historyCodecModel](r)
			if err != nil {
				t.Fatal(err)
			}
			k := &historyLifetimeKernel{readModelKernel: readModelKernel{register: func(context.Context, *contracts.RegisterManyRequest) error { return nil }}}
			client, parent := supervisionClient(t, &supervisedKernel{readModels: k}, WithRegistry(r))
			store, err := client.EventStore(parent, "store")
			if err != nil {
				t.Fatal(err)
			}
			// The test transport has no actual projection. Admission is supplied
			// by this adapter, not advertised as kernel projection evidence.
			service, err := readmodels.New("store", DefaultNamespace, store.ReadModels().Catalog(), &clientTransport{client: client, store: store}, readmodels.WithProjectionReplayValidator(func(context.Context, readmodels.Descriptor) (bool, error) { return true, nil }))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(parent)
			calls := 0
			historyDecodeHook = func() error {
				calls++
				if cancelDuringDecode && calls == 2 {
					cancel()
				}
				if !cancelDuringDecode {
					return client.Close()
				}
				return nil
			}
			reader := readmodels.For(service, model)
			if snapshots {
				result, err := reader.GetSnapshots(ctx, "key")
				if cancelDuringDecode {
					if !errors.Is(err, context.Canceled) || result != nil {
						t.Fatal("partial snapshots after cancellation", err)
					}
				} else if err != nil || len(result) != 2 {
					t.Fatal("decoder close deadlocked or failed", err)
				}
			} else {
				result, err := reader.GetAll(ctx, nil)
				if cancelDuringDecode {
					if !errors.Is(err, context.Canceled) || result.Instances != nil || result.ProcessedEventsCount != 0 {
						t.Fatal("partial collection after cancellation", err)
					}
				} else if err != nil || len(result.Instances) != 2 {
					t.Fatal("decoder close deadlocked or failed", err)
				}
			}
			historyDecodeHook = nil
			cancel()
		}
	}
}
