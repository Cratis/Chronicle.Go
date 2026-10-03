// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
)

func historyTime() *contracts.SerializableDateTimeOffset {
	return &contracts.SerializableDateTimeOffset{Value: "2026-01-02T03:04:05.1234567+02:30"}
}
func historyContribution() *contracts.Event {
	id, _ := metadata.ParseCorrelationID("00112233-4455-6677-8899-aabbccddeeff")
	return &contracts.Event{Content: `{"unknown":"raw"}`, Context: &contracts.EventContext{
		EventType: &contracts.EventType{Id: "unregistered", Generation: 4, Tombstone: true}, EventSourceType: "person", EventSourceId: "owner", SequenceNumber: 91,
		EventStreamType: "details", EventStreamId: "stream", Occurred: historyTime(), CorrelationId: wire.Guid(id),
		Causation: []*contracts.Causation{{Occurred: historyTime(), Type: "command", Properties: map[string]string{"key": "value"}}},
		CausedBy:  &contracts.Identity{Subject: "actor", Name: "Actor", UserName: "actor-login", OnBehalfOf: &contracts.Identity{Subject: "delegate"}},
		Tags:      []string{"tag"}, Hash: "hash", ObservationState: contracts.EventObservationState(99), Subject: "subject",
		NamedTags: []*contracts.NamedTag{{Name: "name", Value: "value"}},
	}}
}

func TestSnapshotsOwnCompleteContributionContextWithoutInventingPresence(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Projection, "projection"), readmodels.WithEventSequence("inbox-origin"))
	event := historyContribution()
	a := wire.Correlation(event.Context.CorrelationId)
	b, _ := metadata.ParseCorrelationID("ffeeddcc-bbaa-9988-7766-554433221100")
	response := &contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, CorrelationId: wire.Guid(b), Data: []*contracts.ReadModelSnapshotResponse{
		{Instance: `{"_id":"owner","name":"plaintext"}`, Occurred: historyTime(), CorrelationId: wire.Guid(a), Events: []*contracts.Event{event}},
		{Instance: `{}`, Occurred: historyTime(), CorrelationId: wire.Guid(b)},
	}}
	service, ctx := serviceFixture(t, &modelKernel{options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)},
		snapshots: func(_ context.Context, r *contracts.AllSnapshotsForReadModelRequest) (*contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
			if r.EventStore != "store" || r.Namespace != "tenant-a" || r.ReadModel != string(model.Identifier()) || r.EventSequenceId != "inbox-origin" || r.ReadModelKey != "owner" || r.Grouping != "Correlation" {
				t.Error("snapshot coordinates")
			}
			return response, nil
		}, release: func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
			t.Error("snapshot double release")
			return nil, errors.New("double release")
		},
	}, model.Descriptor())
	result, err := readmodels.For(service, model).GetSnapshots(ctx, "owner")
	if err != nil || len(result) != 2 || result[0].Instance.ID != "owner" || result[0].Instance.Name != "plaintext" || result[0].CorrelationID != a || result[1].CorrelationID != b || result[1].Instance.ID != "" || result[1].Events == nil {
		t.Fatalf("snapshots=%+v err=%v", result, err)
	}
	wantTime, _ := time.Parse(time.RFC3339Nano, historyTime().Value)
	if !result[0].Occurred.Equal(wantTime) || result[0].Occurred.Format("-07:00") != "+02:30" {
		t.Fatal("occurrence offset lost")
	}
	contribution := result[0].Events[0]
	c := contribution.Context
	if c.Store != "store" || c.Namespace != "tenant-a" || c.Sequence != "inbox-origin" || c.EventType != (events.TypeRef{ID: "unregistered", Generation: 4}) || !c.Tombstone || c.SourceType != "person" || c.SourceID != "owner" || c.SequenceNumber != 91 || c.StreamType != "details" || c.StreamID != "stream" || !c.Occurred.Equal(wantTime) || c.CorrelationID != a || c.Causation[0].Type != "command" || c.Causation[0].Properties["key"] != "value" || c.CausedBy.Subject != "actor" || c.CausedBy.Name != "Actor" || c.CausedBy.UserName != "actor-login" || c.CausedBy.OnBehalfOf.Subject != "delegate" || c.Tags[0] != "tag" || c.Hash != "hash" || c.ObservationState != 99 || c.Subject != "subject" || c.NamedTags[0] != (events.NamedTag{Name: "name", Value: "value"}) {
		t.Fatalf("context=%+v", c)
	}
	if contribution.ID != "" || contribution.OriginalContent != nil || contribution.Revisions != nil || contribution.GenerationalContent != nil || string(contribution.Content) != event.Content {
		t.Fatal("fabricated contribution data")
	}
	c.Causation[0].Properties["key"] = "changed"
	c.CausedBy.OnBehalfOf.Subject = "changed"
	c.Tags[0] = "changed"
	if event.Context.Causation[0].Properties["key"] != "value" || event.Context.CausedBy.OnBehalfOf.Subject != "delegate" || event.Context.Tags[0] != "tag" {
		t.Fatal("borrowed response memory")
	}
}

func TestSnapshotsRejectLateFailuresAndEnvelopeFailures(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Projection, "projection"))
	for _, mutate := range []func(*contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse){
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) { r.IsAuthorized = false },
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) {
			r.ExceptionMessages = []string{"PRIVATE"}
		},
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) {
			r.ValidationResults = []*contracts.ValidationResult{{Severity: contracts.ValidationResultSeverity_Error, Message: "PRIVATE"}}
		},
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) { r.Data[1] = nil },
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) {
			r.Data[1].Instance = `{"count":"PRIVATE"}`
		},
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) {
			r.Data[1].Occurred.Value = "PRIVATE"
		},
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) {
			r.Data[1].Events[0].Context.Occurred = nil
		},
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) {
			r.Data[1].Events[0].Context.SequenceNumber = uint64(events.Unavailable)
		},
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) {
			r.Data[1].Events[0].Content = "PRIVATE"
		},
		func(r *contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse) {
			r.Data[1].Events[0].Context.NamedTags = []*contracts.NamedTag{nil}
		},
	} {
		response := &contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*contracts.ReadModelSnapshotResponse{{Instance: `{}`, Occurred: historyTime()}, {Instance: `{}`, Occurred: historyTime(), Events: []*contracts.Event{historyContribution()}}}}
		mutate(response)
		service, ctx := serviceFixture(t, &modelKernel{options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)}, snapshots: func(context.Context, *contracts.AllSnapshotsForReadModelRequest) (*contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
			return response, nil
		}}, model.Descriptor())
		result, err := readmodels.For(service, model).GetSnapshots(ctx, "key")
		if err == nil || result != nil || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("partial result=%+v err=%v", result, err)
		}
	}
}

func TestSnapshotsRefuseReducersBeforeRPCAndReturnOwnedEmpty(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Reducer, "reducer"))
	service, ctx := serviceFixture(t, &modelKernel{}, model.Descriptor())
	if result, err := service.GetSnapshots(ctx, model.Identifier(), "source"); result != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatal(result, err)
	}
	projection := person(t, readmodels.WithObserver(readmodels.Projection, "projection"))
	service, ctx = serviceFixture(t, &modelKernel{options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)}, snapshots: func(context.Context, *contracts.AllSnapshotsForReadModelRequest) (*contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
		return &contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true}, nil
	}}, projection.Descriptor())
	result, err := service.GetSnapshots(ctx, projection.Identifier(), "absent")
	if err != nil || result == nil || len(result) != 0 {
		t.Fatal("empty snapshots", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if result, err := service.GetSnapshots(canceled, projection.Identifier(), "absent"); result != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(result, err)
	}
}
