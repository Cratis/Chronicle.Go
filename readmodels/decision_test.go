// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/compliance"
	projections "github.com/cratis/chronicle.go/contracts/projections"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type decisionChanged struct{ Name string }
type decisionRemoved struct{}
type decisionPerson struct {
	ID    string   `json:"id"`
	Name  string   `json:"name"`
	Items []string `json:"items"`
}

type decisionFixture struct {
	t                  *testing.T
	catalog            *decision.Catalog
	model              Model[decisionPerson]
	reader             *DecisionReader[decisionPerson]
	sequence           *eventsequences.Sequence
	requests           []proto.Message
	handle             func(context.Context, any) (proto.Message, error)
	invalid            atomic.Bool
	acquired, released int
}

func newDecisionFixture(t *testing.T, options ...ModelOption) *decisionFixture {
	t.Helper()
	changed, err := events.Define[decisionChanged](events.WithID("changed"))
	if err != nil {
		t.Fatal(err)
	}
	removed, err := events.Define[decisionRemoved](events.WithID("removed"))
	if err != nil {
		t.Fatal(err)
	}
	eventTypes, err := events.NewCatalog(changed.Descriptor(), removed.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	model, err := Define[decisionPerson](append([]ModelOption{WithIdentifier("person"), WithObserver(Projection, "people")}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	models, err := NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	f := &decisionFixture{t: t, model: model}
	p := &projections.ProjectionDefinition{Identifier: "people", ReadModel: "person", EventSequenceId: string(events.EventLog), All: &projections.FromEveryDefinition{}, From: []*projections.KeyValuePair_EventType_FromDefinition{{Key: &projections.EventType{Id: "changed", Generation: 1}, Value: &projections.FromDefinition{}}}, RemovedWith: []*projections.KeyValuePair_EventType_RemovedWithDefinition{{Key: &projections.EventType{Id: "removed", Generation: 1}, Value: &projections.RemovedWithDefinition{}}}}
	f.catalog = decision.NewCatalog([]*projections.ProjectionDefinition{p}, eventTypes)
	service, err := New("store", "tenant", models, f)
	if err != nil {
		t.Fatal(err)
	}
	f.reader = DecisionsFor(service, model)
	f.sequence, err = eventsequences.New("store", "tenant", events.EventLog, eventTypes, f)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *decisionFixture) DecisionCatalog() *decision.Catalog { return f.catalog }
func (f *decisionFixture) DecisionTarget(sequence events.SequenceID) decision.Target {
	return decision.Target{Client: f, Store: "store", Namespace: "tenant", Sequence: string(sequence)}
}
func (f *decisionFixture) AcquireDecision(ctx context.Context) (*decision.Lease, error) {
	f.acquired++
	var released bool
	return &decision.Lease{Context: ctx, Conn: f, Generation: 1, Check: func() error {
		if f.invalid.Load() {
			return decision.ErrStale
		}
		return nil
	}, Release: func() {
		if !released {
			f.released++
			released = true
		}
	}}, nil
}
func (f *decisionFixture) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected stream")
}
func (f *decisionFixture) Invoke(ctx context.Context, _ string, request, reply any, _ ...grpc.CallOption) error {
	f.requests = append(f.requests, proto.Clone(request.(proto.Message)))
	var response proto.Message
	var err error
	if f.handle != nil {
		response, err = f.handle(ctx, request)
	}
	if response == nil && err == nil {
		response = f.respond(request)
	}
	if err == nil {
		proto.Merge(reply.(proto.Message), response)
	}
	return err
}
func (f *decisionFixture) respond(request any) proto.Message {
	switch request := request.(type) {
	case *contracts.GetDefinitionsRequest:
		return &contracts.GetDefinitionsResponse{ReadModels: []*contracts.ReadModelDefinition{{Type: &contracts.ReadModelType{Identifier: "person", Generation: 1}, Schema: f.model.Descriptor().Schema(), ObserverType: contracts.ReadModelObserverType_Projection, ObserverIdentifier: "people"}}}
	case *projections.GetAllDefinitionsRequest:
		return &projections.IEnumerable_ProjectionDefinition{Items: []*projections.ProjectionDefinition{proto.CloneOf(f.catalog.Projections[0])}}
	case *sequences.TailSequenceNumberRequest:
		return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: 5}}
	case *contracts.GetInstanceByKeyRequest:
		return &contracts.GetInstanceByKeyResponse{ReadModel: `{"id":"source","name":"Ada"}`, LastHandledEventSequenceNumber: 5}
	case *contracts.DehydrateSessionRequest:
		return &emptypb.Empty{}
	case *sequences.AppendManyForEventSourcesRequest:
		positions := make([]uint64, len(request.Events))
		for i := range positions {
			positions[i] = uint64(i + 6)
		}
		return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: positions, ConcurrencyCheckPerformed: true}}
	default:
		f.t.Fatalf("unexpected decision RPC %T", request)
		return nil
	}
}

func TestDecisionAdmissionMatrix(t *testing.T) {
	cases := []struct {
		name   string
		change func(*decisionFixture)
		reason DecisionReadRefusalReason
	}{
		{"passive", func(f *decisionFixture) { f.catalog.Projections[0].IsActive = false }, ""},
		{"active", func(f *decisionFixture) { f.catalog.Projections[0].IsActive = true }, ""},
		{"finite-all", func(f *decisionFixture) {
			f.catalog.Projections[0].All.Properties = map[string]string{"stamp": "$eventContext.occurred"}
		}, ""},
		{"missing-projection", func(f *decisionFixture) { f.catalog.Projections = nil }, DecisionAmbiguousProjection},
		{"duplicate", func(f *decisionFixture) {
			f.catalog.Projections = append(f.catalog.Projections, f.catalog.Projections[0])
		}, DecisionAmbiguousProjection},
		{"sequence", func(f *decisionFixture) { f.catalog.Projections[0].EventSequenceId = "other" }, DecisionNotEventLog},
		{"join", func(f *decisionFixture) {
			f.catalog.Projections[0].Join = []*projections.KeyValuePair_EventType_JoinDefinition{{}}
		}, DecisionJoin},
		{"removal-join", func(f *decisionFixture) {
			f.catalog.Projections[0].RemovedWithJoin = []*projections.KeyValuePair_EventType_RemovedWithJoinDefinition{{}}
		}, DecisionJoin},
		{"child", func(f *decisionFixture) {
			f.catalog.Projections[0].Children = map[string]*projections.ChildrenDefinition{"children": {}}
		}, DecisionHierarchy},
		{"nested", func(f *decisionFixture) {
			f.catalog.Projections[0].Nested = map[string]*projections.ChildrenDefinition{"child": {}}
		}, DecisionHierarchy},
		{"all-events", func(f *decisionFixture) { f.catalog.Projections[0].SubscribesToAllEvents = true }, DecisionOpenEndedEventTypes},
		{"derivative", func(f *decisionFixture) {
			f.catalog.Projections[0].FromEvery = []*projections.FromDerivativesDefinition{{}}
		}, DecisionDerivatives},
		{"event-property", func(f *decisionFixture) {
			f.catalog.Projections[0].FromEventProperty = &projections.FromEventPropertyDefinition{}
		}, DecisionFromEventProperty},
		{"from-key", func(f *decisionFixture) { f.catalog.Projections[0].From[0].Value.Key = "other" }, DecisionNotEventSourceKeyed},
		{"from-parent", func(f *decisionFixture) { f.catalog.Projections[0].From[0].Value.ParentKey = "other" }, DecisionNotEventSourceKeyed},
		{"removal-key", func(f *decisionFixture) { f.catalog.Projections[0].RemovedWith[0].Value.Key = "other" }, DecisionNotEventSourceKeyed},
		{"removal-parent", func(f *decisionFixture) { f.catalog.Projections[0].RemovedWith[0].Value.ParentKey = "other" }, DecisionNotEventSourceKeyed},
		{"all-key", func(f *decisionFixture) { f.catalog.Projections[0].All.Key = "other" }, DecisionNotEventSourceKeyed},
		{"no-events", func(f *decisionFixture) {
			f.catalog.Projections[0].From = nil
			f.catalog.Projections[0].RemovedWith = nil
		}, DecisionNoEventTypes},
		{"comma", func(f *decisionFixture) { f.catalog.Projections[0].From[0].Key.Id = "changed,removed" }, DecisionUnsupportedEventType},
		{"unknown", func(f *decisionFixture) { f.catalog.Projections[0].From[0].Key.Id = "unknown" }, DecisionUnsupportedEventType},
		{"unknown-generation", func(f *decisionFixture) { f.catalog.Projections[0].From[0].Key.Generation = 2 }, DecisionUnsupportedEventType},
		{"malformed-from", func(f *decisionFixture) { f.catalog.Projections[0].From[0] = nil }, DecisionUnsupportedEventType},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newDecisionFixture(t)
			tc.change(f)
			got := f.reader.Admit()
			if got.Reason != tc.reason || got.IsAdmitted != (tc.reason == "") {
				t.Fatalf("admission: %+v", got)
			}
			if tc.reason != "" {
				read, err := f.reader.GetDetached(t.Context(), "source")
				if !errors.Is(err, ErrDecisionReadRefused) || !read.Token.IsZero() {
					t.Fatalf("read: %+v %v", read, err)
				}
			}
			if len(f.requests) != 0 {
				t.Fatal("local admission used RPC")
			}
		})
	}
	f := newDecisionFixture(t, WithObserver(Reducer, "reducer"))
	if f.reader.Admit().Reason != DecisionReducer {
		t.Fatal("reducer admitted")
	}
}

func TestDecisionKeySchemaAndCanonicalKeys(t *testing.T) {
	for _, tc := range []struct {
		schema string
		valid  bool
	}{
		{`{"properties":{"id":{"type":"string"}}}`, true},
		{`{"properties":{"Id":{"type":"string","format":"guid"}}}`, true},
		{`{"properties":{"id":{"$ref":"#/definitions/key"}},"definitions":{"key":{"type":"string","format":"uuid"}}}`, true},
		{`{"properties":{"ID":{"type":"string"}}}`, false},
		{`{"properties":{"id":{"type":"integer"}}}`, false},
		{`{"properties":{"id":{"type":"string","format":"date"}}}`, false},
		{`{"properties":{"id":{"type":["string","null"]}}}`, true},
		{`{"properties":{"id":{"type":"string","format":"uuid?"}}}`, false},
		{`{"properties":{"id":{"$ref":"#/definitions/key"}},"definitions":{"key":{"$ref":"#/definitions/key"}}}`, false},
		{`{}`, false},
	} {
		_, ok := decisionKeySchema(tc.schema)
		if ok != tc.valid {
			t.Errorf("schema %s: %v", tc.schema, ok)
		}
	}
	for _, key := range []Key{"", " ", " source", "source ", "*", "a#b", "\t"} {
		f := newDecisionFixture(t)
		if read, err := f.reader.GetDetached(t.Context(), key); !errors.Is(err, ErrDecisionReadRefused) || !read.Token.IsZero() || len(f.requests) != 0 {
			t.Fatalf("unsafe key %q: %+v %v", key, read, err)
		}
	}
	for _, key := range []Key{"00112233-4455-6677-8899-aabbccddeeff", "00112233-4455-6677-8899-AABBCCDDEEFF", "00112233445566778899aabbccddeeff", "{00112233-4455-6677-8899-aabbccddeeff}", "source"} {
		if got := validDecisionKey(key, keyShape{format: "uuid"}); got != (key == "00112233-4455-6677-8899-aabbccddeeff") {
			t.Errorf("UUID key %q: %v", key, got)
		}
	}
}

func TestDecisionRetriesFreshSessionsAndUsesUnfilteredBoundary(t *testing.T) {
	f := newDecisionFixture(t)
	folds := 0
	f.handle = func(_ context.Context, request any) (proto.Message, error) {
		switch req := request.(type) {
		case *sequences.TailSequenceNumberRequest:
			position := uint64(10)
			if req.EventSourceId != "" {
				position = 5
				if req.EventSourceId != "source" || req.EventTypeIds != "changed,removed" {
					t.Fatalf("probe: %+v", req)
				}
			} else if req.EventTypeIds != "" {
				t.Fatal("filtered boundary")
			}
			return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: position}}, nil
		case *contracts.GetInstanceByKeyRequest:
			folds++
			last := []uint64{4, 11, 5}[folds-1]
			return &contracts.GetInstanceByKeyResponse{ReadModel: `{"id":"source","items":null}`, LastHandledEventSequenceNumber: last}, nil
		}
		return nil, nil
	}
	read, err := f.reader.GetDetached(t.Context(), "source")
	if err != nil || read.Token.IsZero() || !read.Instance.Exists || read.Instance.Value.Items == nil || folds != 3 {
		t.Fatalf("read: %+v %v folds=%d", read, err, folds)
	}
	var sessions []string
	tails := 0
	for _, request := range f.requests {
		if _, ok := request.(*sequences.TailSequenceNumberRequest); ok {
			tails++
		}
		if req, ok := request.(*contracts.GetInstanceByKeyRequest); ok {
			sessions = append(sessions, req.SessionId)
		}
	}
	if tails != 6 {
		t.Fatalf("fresh tails = %d, want 6", tails)
	}
	if len(slices.Compact(slices.Clone(sessions))) != 3 {
		t.Fatal("session reused")
	}
	assertDecisionCleanup(t, f)
	unit, owner, err := transactions.Begin(t.Context(), f.sequence)
	if err != nil {
		t.Fatal(err)
	}
	if err = unit.Enroll(read.Token); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	request := f.requests[len(f.requests)-1].(*sequences.AppendManyForEventSourcesRequest)
	if len(request.Events) != 0 || len(request.ConcurrencyScopes) != 1 || request.ConcurrencyScopes[0].Scope.SequenceNumber != 10 || request.ConcurrencyScopes[0].Scope.ExpectsNoMatchingEvent {
		t.Fatalf("boundary: %+v", request)
	}
	if f.acquired != f.released {
		t.Fatal("lease leaked")
	}
}
func assertDecisionCleanup(t *testing.T, f *decisionFixture) {
	t.Helper()
	var reads []*contracts.GetInstanceByKeyRequest
	var cleanups []*contracts.DehydrateSessionRequest
	for _, request := range f.requests {
		if req, ok := request.(*contracts.GetInstanceByKeyRequest); ok {
			reads = append(reads, req)
		}
		if req, ok := request.(*contracts.DehydrateSessionRequest); ok {
			cleanups = append(cleanups, req)
		}
	}
	if len(reads) != len(cleanups) {
		t.Fatalf("sessions=%d cleanup=%d", len(reads), len(cleanups))
	}
	for i, read := range reads {
		cleanup := cleanups[i]
		if cleanup.SessionId != read.SessionId || cleanup.ReadModelKey != read.ReadModelKey || cleanup.ReadModelIdentifier != read.ReadModelIdentifier || cleanup.EventSequenceId != read.EventSequenceId || cleanup.EventStore != read.EventStore || cleanup.Namespace != read.Namespace {
			t.Fatal("cleanup changed coordinates")
		}
	}
}

func TestDecisionAttemptExhaustionAndPresence(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		boundary, probe, last uint64
		json                  string
		reason                DecisionReadRefusalReason
		exists                bool
		folds                 int
	}{
		{"incomplete", 5, 5, 4, `{}`, DecisionFoldIncomplete, false, 3},
		// C# 2e31b0dfb returns its third ahead fold at boundary 5. Go refuses it.
		{"third-ahead", 5, 6, 6, `{}`, DecisionFoldAhead, false, 3},
		{"initial-state", ^uint64(0), ^uint64(0), ^uint64(0), `{"name":"initial"}`, "", false, 1},
		{"absent-nonempty-log", 5, ^uint64(0), ^uint64(0), `{"name":"initial"}`, "", false, 1},
		{"removed", 5, 5, 5, `null`, "", false, 1},
		{"first-event", 0, 0, 0, `{"id":"source"}`, "", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDecisionFixture(t)
			folds := 0
			f.handle = func(_ context.Context, request any) (proto.Message, error) {
				switch req := request.(type) {
				case *sequences.TailSequenceNumberRequest:
					position := tc.boundary
					if req.EventSourceId != "" {
						position = tc.probe
					}
					return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: position}}, nil
				case *contracts.GetInstanceByKeyRequest:
					folds++
					return &contracts.GetInstanceByKeyResponse{ReadModel: tc.json, LastHandledEventSequenceNumber: tc.last}, nil
				}
				return nil, nil
			}
			read, err := f.reader.GetDetached(t.Context(), "source")
			if tc.reason != "" {
				var refusal *DecisionReadRefused
				if !errors.As(err, &refusal) || refusal.Reason != tc.reason || !read.Token.IsZero() {
					t.Fatalf("refusal: %+v %v", read, err)
				}
			} else if err != nil || read.Token.IsZero() || read.Instance.Exists != tc.exists {
				t.Fatalf("read: %+v %v", read, err)
			}
			if folds != tc.folds {
				t.Fatalf("folds=%d", folds)
			}
			if tc.name == "removed" && read.Instance.LastHandled == nil {
				t.Fatal("removal lost progress")
			}
			assertDecisionCleanup(t, f)
			if tc.name == "initial-state" {
				u, o, err := transactions.Begin(t.Context(), f.sequence)
				if err != nil {
					t.Fatal(err)
				}
				if err = u.Enroll(read.Token); err != nil {
					t.Fatal(err)
				}
				if _, err = o.Commit(t.Context()); err != nil {
					t.Fatal(err)
				}
				req := f.requests[len(f.requests)-1].(*sequences.AppendManyForEventSourcesRequest)
				if !req.ConcurrencyScopes[0].Scope.ExpectsNoMatchingEvent || req.ConcurrencyScopes[0].Scope.SequenceNumber != ^uint64(0) {
					t.Fatal("absence became unchecked")
				}
			}
		})
	}
}

func TestDecisionFailuresAwaitCleanupAndReturnNoToken(t *testing.T) {
	for _, failure := range []string{"read", "decode", "release", "cleanup", "cancel", "epoch", "generation", "reserved-last", "reserved-tail"} {
		t.Run(failure, func(t *testing.T) {
			f := newDecisionFixture(t, WithPII("name"))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			correlation, err := metadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			ctx = metadata.WithCorrelation(grpcmetadata.AppendToOutgoingContext(ctx, "test-metadata", "retained"), correlation)
			cause := errors.New("injected failure")
			folds, cleanups := 0, 0
			f.handle = func(call context.Context, request any) (proto.Message, error) {
				switch req := request.(type) {
				case *sequences.TailSequenceNumberRequest:
					if failure == "reserved-tail" {
						return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: ^uint64(0) - 1}}, nil
					}
				case *contracts.GetInstanceByKeyRequest:
					folds++
					switch failure {
					case "read":
						return nil, cause
					case "decode":
						return &contracts.GetInstanceByKeyResponse{ReadModel: `{"id":"source","name":32}`, LastHandledEventSequenceNumber: 5}, nil
					case "cancel":
						cancel()
						return nil, ctx.Err()
					case "epoch":
						f.catalog.Epoch.Add(1)
					case "generation":
						f.invalid.Store(true)
					case "reserved-last":
						return &contracts.GetInstanceByKeyResponse{ReadModel: `{}`, LastHandledEventSequenceNumber: ^uint64(0) - 2}, nil
					}
				case *compliance.ReleaseRequest:
					if failure == "release" {
						return nil, cause
					}
					return &compliance.ReleaseResponse{Payload: req.Payload}, nil
				case *contracts.DehydrateSessionRequest:
					cleanups++
					deadline, ok := call.Deadline()
					if !ok || time.Until(deadline) > 5*time.Second || call.Err() != nil {
						t.Fatal("cleanup did not detach/bound cancellation")
					}
					md, _ := grpcmetadata.FromOutgoingContext(call)
					if md.Get("test-metadata")[0] != "retained" || metadata.Correlation(call) != correlation {
						t.Fatal("cleanup lost metadata")
					}
					if failure == "cleanup" {
						return nil, cause
					}
				}
				return nil, nil
			}
			read, err := f.reader.GetDetached(ctx, "source")
			if err == nil || !read.Token.IsZero() || read.Instance.Exists {
				t.Fatalf("failed open: %+v %v", read, err)
			}
			if cleanups != folds || folds > 1 {
				t.Fatalf("folds=%d cleanup=%d", folds, cleanups)
			}
			assertDecisionCleanup(t, f)
			if f.acquired != f.released {
				t.Fatal("lease leaked")
			}
		})
	}
}

func TestDecisionAgreementRechecksServerShapeAndKey(t *testing.T) {
	for _, change := range []string{"observer", "duplicate-model", "missing-projection", "key", "parent", "all", "types", "hierarchy", "mapping-only"} {
		t.Run(change, func(t *testing.T) {
			f := newDecisionFixture(t)
			folds := 0
			f.handle = func(_ context.Context, request any) (proto.Message, error) {
				switch request.(type) {
				case *contracts.GetInstanceByKeyRequest:
					folds++
				case *contracts.GetDefinitionsRequest:
					response := f.respond(request).(*contracts.GetDefinitionsResponse)
					if folds > 0 {
						switch change {
						case "observer":
							response.ReadModels[0].ObserverIdentifier = "other"
						case "duplicate-model":
							response.ReadModels = append(response.ReadModels, response.ReadModels[0])
						case "key":
							response.ReadModels[0].Schema = `{"properties":{"id":{"type":"integer"}}}`
						}
					}
					return response, nil
				case *projections.GetAllDefinitionsRequest:
					response := f.respond(request).(*projections.IEnumerable_ProjectionDefinition)
					if folds > 0 {
						switch change {
						case "missing-projection":
							response.Items = nil
						case "parent":
							response.Items[0].From[0].Value.ParentKey = "other"
						case "all":
							response.Items[0].All.Key = "other"
						case "types":
							response.Items[0].RemovedWith = nil
						case "hierarchy":
							response.Items[0].Nested = map[string]*projections.ChildrenDefinition{"child": {}}
						case "mapping-only":
							response.Items[0].From[0].Value.Properties = map[string]string{"name": "different"}
						}
					}
					return response, nil
				}
				return nil, nil
			}
			read, err := f.reader.GetDetached(t.Context(), "source")
			if change == "mapping-only" {
				if err != nil || read.Token.IsZero() {
					t.Fatalf("imagined full-definition equality: %v", err)
				}
			} else {
				var refused *DecisionReadRefused
				if !errors.As(err, &refused) || refused.Reason != DecisionDefinitionMismatch || !read.Token.IsZero() {
					t.Fatalf("agreement: %+v %v", read, err)
				}
			}
			assertDecisionCleanup(t, f)
		})
	}
}

func TestDecisionGetRequiresAndEnrollsExistingParticipant(t *testing.T) {
	f := newDecisionFixture(t)
	if _, err := f.reader.Get(t.Context(), "source"); !errors.Is(err, ErrDecisionRequiresUnitOfWork) || len(f.requests) != 0 {
		t.Fatalf("implicit unit: %v", err)
	}
	u, o, err := transactions.Begin(t.Context(), f.sequence)
	if err != nil {
		t.Fatal(err)
	}
	read, err := f.reader.Get(transactions.WithUnitOfWork(t.Context(), u), "source")
	if err != nil || read.Token.IsZero() {
		t.Fatal(err)
	}
	if err = o.Rollback(); err != nil {
		t.Fatal(err)
	}
	other, _, err := transactions.Begin(t.Context(), f.sequence)
	if err != nil {
		t.Fatal(err)
	}
	if err = other.Enroll(read.Token); !errors.Is(err, transactions.ErrDecisionOwner) {
		t.Fatalf("read not enrolled: %v", err)
	}
	if read, err = f.reader.Get(transactions.WithUnitOfWork(t.Context(), u), "source"); !errors.Is(err, transactions.ErrCompleted) || !read.Token.IsZero() {
		t.Fatalf("terminal enrollment: %+v %v", read, err)
	}
}

func TestDecisionLowLevelServiceCannotIssue(t *testing.T) {
	f := newDecisionFixture(t)
	f.reader.reader.service.decisions = nil
	if f.reader.Admit().IsAdmitted {
		t.Fatal("low-level transport admitted")
	}
	if _, err := f.reader.GetDetached(t.Context(), "source"); !errors.Is(err, ErrDecisionReadRefused) {
		t.Fatal(err)
	}
}
