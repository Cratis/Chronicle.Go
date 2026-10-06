// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	models "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type materializedEvidence struct {
	contracts.UnimplementedObserversServer
	contracts.UnimplementedFailedPartitionsServer
	models.UnimplementedReadModelsServer
	calls           []string
	request         *contracts.WaitForObserverCompletionRequest
	missing, failed bool
	missingOnce     bool
	position        uint64
}

func (k *materializedEvidence) WaitForCompletion(_ context.Context, r *contracts.WaitForObserverCompletionRequest) (*contracts.WaitForObserverCompletionResponse, error) {
	k.calls = append(k.calls, "wait")
	k.request = r
	return &contracts.WaitForObserverCompletionResponse{IsSuccess: true}, nil
}
func (k *materializedEvidence) GetObserverInformation(_ context.Context, r *contracts.GetObserverInformationRequest) (*contracts.ObserverInformation, error) {
	k.calls = append(k.calls, "observer")
	if k.missing || k.missingOnce {
		k.missingOnce = false
		return nil, status.Error(codes.NotFound, "missing")
	}
	return &contracts.ObserverInformation{Id: r.ObserverId, EventSequenceId: r.EventSequenceId, LastHandledEventSequenceNumber: k.position}, nil
}
func (k *materializedEvidence) GetFailedPartitions(context.Context, *contracts.GetFailedPartitionsRequest) (*contracts.IEnumerable_FailedPartition, error) {
	k.calls = append(k.calls, "failures")
	if k.failed {
		return &contracts.IEnumerable_FailedPartition{Items: []*contracts.FailedPartition{{Id: wire.Guid(metadata.CorrelationID(uuid.New())), ObserverId: "strict-selected", Partition: "source"}}}, nil
	}
	return &contracts.IEnumerable_FailedPartition{}, nil
}
func (k *materializedEvidence) GetAllInstances(_ context.Context, r *models.GetAllInstancesRequest) (*models.GetAllInstancesResponse, error) {
	k.calls = append(k.calls, "sink")
	if r.EventCount != uint64(events.UnlimitedCount) {
		return nil, errors.New("not unlimited sink route")
	}
	return &models.GetAllInstancesResponse{Instances: []string{`{"id":"source","name":"from real sink"}`}}, nil
}
func materializedFixture(t *testing.T) (subscriptionFixture, *materializedEvidence) {
	t.Helper()
	f := newSubscriptionFixture(t, false, "ordinary")
	k := &materializedEvidence{}
	contracts.RegisterObserversServer(f.connection.substituteTransport, k)
	contracts.RegisterFailedPartitionsServer(f.connection.substituteTransport, k)
	models.RegisterReadModelsServer(f.connection.substituteTransport, k)
	f.scenario.materialized = true
	var err error
	f.scenario.membership, err = strictProjectionSubscription(f.scenario.projection, f.scenario.artifacts.Events)
	if err != nil {
		t.Fatal(err)
	}
	return f, k
}
func TestMaterializedScenarioWaitsForCompletionThenReadsUnlimitedSink(t *testing.T) {
	f, k := materializedFixture(t)
	if err := f.scenario.Given(t.Context(), "source", subscriptionAdded{Name: "seed"}, subscriptionMarker{}, subscriptionRemoved{}); err != nil {
		t.Fatal(err)
	}
	k.position = 2
	got, err := f.scenario.Instances(t.Context())
	if err != nil || got["source"].Name != "from real sink" {
		t.Fatalf("sink = %+v, %v", got, err)
	}
	if !reflect.DeepEqual(k.calls, []string{"wait", "observer", "failures", "sink"}) {
		t.Fatalf("order: %v", k.calls)
	}
	r := k.request
	if r == nil || r.TailEventSequenceNumber != 2 || !r.HasFirstEventSequenceNumber || r.FirstEventSequenceNumber != 0 || r.EventStore != "strict-store" || r.Namespace != "strict-ns" || r.EventSequenceId != string(f.scenario.projection.EventSequence()) || len(r.EventTypeTails) != 3 {
		t.Fatalf("completion: %+v", r)
	}
	for i, tail := range r.EventTypeTails {
		descriptor, _ := f.scenario.artifacts.Events.Lookup([]any{subscriptionAdded{}, subscriptionMarker{}, subscriptionRemoved{}}[i])
		if tail.SequenceNumber != uint64(i) || tail.EventType.Id != string(descriptor.Ref().ID) || tail.EventType.Generation != uint32(descriptor.Ref().Generation) {
			t.Fatalf("tail: %+v", tail)
		}
	}
}
func TestMaterializedScenarioRefusesMissingObserverOrFailedPartitions(t *testing.T) {
	for _, mode := range []string{"missing", "failed", "behind"} {
		t.Run(mode, func(t *testing.T) {
			f, k := materializedFixture(t)
			if err := f.scenario.Given(t.Context(), "source", subscriptionAdded{}, subscriptionAdded{}); err != nil {
				t.Fatal(err)
			}
			k.missing, k.failed = mode == "missing", mode == "failed"
			k.position = 1
			if mode == "behind" {
				k.position = 0
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			got, err := f.scenario.Instances(ctx)
			if got != nil || !errors.Is(err, ErrMaterializationIncomplete) {
				t.Fatalf("result: %+v, %v", got, err)
			}
			for _, call := range k.calls {
				if call == "sink" {
					t.Fatal("read partial sink")
				}
			}
		})
	}
}
func TestMaterializedScenarioRefusesForeignSequencePosition(t *testing.T) {
	f, _ := materializedFixture(t)
	if _, err := f.scenario.eventScenario.Store.EventLog().Append(t.Context(), "foreign", subscriptionAdded{}); err != nil {
		t.Fatal(err)
	}
	err := f.scenario.Given(t.Context(), "source", subscriptionAdded{})
	if !errors.Is(err, ErrFidelityUnavailable) || len(f.scenario.history) != 0 {
		t.Fatalf("foreign sequence: %v", err)
	}
	if got, err := f.scenario.Instances(t.Context()); got != nil || !errors.Is(err, ErrFidelityUnavailable) {
		t.Fatalf("foreign sequence read returned state: %+v %v", got, err)
	}
}
func TestMaterializedScenarioFidelity(t *testing.T) {
	f, k := materializedFixture(t)
	if err := f.scenario.Fidelity().Require(ReadModelStorage, ProjectionExecution, DurableStorage); err != nil {
		t.Fatal(err)
	}
	for _, layer := range []Layer{ObserverLifecycle, DeliveryMetadata, EffectAcceptance} {
		if !errors.Is(f.scenario.Fidelity().Require(layer), ErrFidelityUnavailable) {
			t.Fatal("claimed", layer)
		}
	}
	got, err := f.scenario.Instances(t.Context())
	if err != nil || got == nil || len(got) != 0 || len(k.calls) != 0 {
		t.Fatalf("empty history read: %+v %v %v", got, err, k.calls)
	}
}
func TestStrictProjectionMembershipIgnoresInitialState(t *testing.T) {
	r := defaultsSubscriptionRegistry(t)
	c, err := chronicle.NewClient(chronicle.WithRegistry(r))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}()
	a, err := c.Artifacts("defaults")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := strictProjectionSubscription(a.Projections[0], a.Events)
	if err != nil || selected.admit("subscriptionAdded") != nil {
		t.Fatalf("defaults membership: %v", err)
	}
}
