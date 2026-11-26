// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package alerts_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/alerts"
	contracts "github.com/cratis/chronicle.go/contracts/alerts"
	"github.com/cratis/chronicle.go/contracts/bcl"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type fakeConn struct {
	grpc.ClientConnInterface
	method  string
	request proto.Message
	reply   proto.Message
	err     error
}

func (f *fakeConn) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	f.method, f.request = method, proto.Clone(args.(proto.Message))
	if f.err != nil {
		return f.err
	}
	proto.Merge(reply.(proto.Message), f.reply)
	return nil
}

func service(t *testing.T, conn *fakeConn) *alerts.Service {
	t.Helper()
	s, err := alerts.New("store", "tenant", conn)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func guid(id uuid.UUID) *bcl.Guid { return wire.Guid(metadata.CorrelationID(id)) }

func details(id uuid.UUID) *contracts.AlertIncidentDetails {
	at := &contracts.SerializableDateTimeOffset{Value: "2024-01-02T03:04:05Z"}
	return &contracts.AlertIncidentDetails{
		Id: guid(id), Target: &contracts.AlertTarget{EventStore: "store", Namespace: "tenant", ObserverId: "obs", EventSequenceId: "event-log", Partition: "p"},
		Condition: "failing", Severity: contracts.AlertSeverity_Critical,
		Evidence: &contracts.AlertEvidence{AttemptCount: 3, FirstFailure: at, LastFailure: at, FailureKind: contracts.FailureKind_Timeout, Message: "m"},
		RaisedAt: at, LastChangedAt: at, RaisedSequenceNumber: 7, LastTransitionSequenceNumber: 9,
	}
}

func lookupConn(status contracts.AlertIncidentsReadinessState, incident *contracts.AlertIncidentDetails) *fakeConn {
	return &fakeConn{reply: &contracts.QueryResult_AlertIncidentLookupResponse{IsAuthorized: true, Data: &contracts.AlertIncidentLookupResponse{Status: status, Incident: incident}}}
}

func pageConn(data *contracts.AlertIncidentPageResponse) *fakeConn {
	return &fakeConn{reply: &contracts.QueryResult_AlertIncidentPageResponse{IsAuthorized: true, Data: data}}
}

func summaryConn(data *contracts.AlertIncidentSummaryResponse) *fakeConn {
	return &fakeConn{reply: &contracts.QueryResult_AlertIncidentSummaryResponse{IsAuthorized: true, Data: data}}
}

func countsOf(counts ...*contracts.AlertIncidentCountDetails) *contracts.AlertIncidentSummaryResponse {
	return &contracts.AlertIncidentSummaryResponse{Counts: counts}
}

func requireProtocolFailure(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, faults.ErrProtocol) {
		t.Fatalf("err = %v", err)
	}
}

func TestGetOpenIncidentDecodesIncidentAndKeepsReadiness(t *testing.T) {
	id := uuid.New()
	conn := lookupConn(contracts.AlertIncidentsReadinessState_Ready, details(id))
	lookup, err := service(t, conn).GetOpenIncident(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	want := &contracts.GetOpenIncidentRequest{EventStore: "store", Namespace: "tenant", IncidentId: guid(id)}
	if !proto.Equal(conn.request, want) {
		t.Fatalf("request = %v", conn.request)
	}
	got := lookup.Incident
	if lookup.Readiness != alerts.Ready || got == nil || got.ID != id || got.Severity != alerts.Critical || got.Evidence.Kind != alerts.Timeout || got.RaisedSequenceNumber != 7 || !got.RaisedAt.Equal(time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("lookup = %+v", lookup)
	}
}

func TestLookupRejectsMismatchedTargetOrID(t *testing.T) {
	cases := map[string]func(*contracts.AlertIncidentDetails){
		"store":     func(row *contracts.AlertIncidentDetails) { row.Target.EventStore = "Store" },
		"namespace": func(row *contracts.AlertIncidentDetails) { row.Target.Namespace = "tenant " },
		"id":        func(row *contracts.AlertIncidentDetails) { row.Id = guid(uuid.New()) },
	}
	for name, alter := range cases {
		t.Run(name, func(t *testing.T) {
			id := uuid.New()
			row := details(id)
			alter(row)
			conn := lookupConn(contracts.AlertIncidentsReadinessState_Ready, row)
			lookup, err := service(t, conn).GetOpenIncident(t.Context(), id)
			requireProtocolFailure(t, err)
			if lookup != (alerts.Lookup{}) {
				t.Fatalf("lookup = %+v", lookup)
			}
		})
	}
}

func TestGetOpenIncidentMissingIncidentIsNilNotProtocolFailure(t *testing.T) {
	conn := lookupConn(contracts.AlertIncidentsReadinessState_CatchingUp, nil)
	lookup, err := service(t, conn).GetOpenIncident(context.Background(), uuid.New())
	if err != nil || lookup.Incident != nil || lookup.Readiness != alerts.CatchingUp {
		t.Fatalf("lookup = %+v, err = %v", lookup, err)
	}
}

func TestMissingDataIsProtocolFailureAndUnauthorizedNeverSucceeds(t *testing.T) {
	missing := pageConn(nil)
	_, err := service(t, missing).GetOpenIncidents(context.Background(), alerts.Query{})
	requireProtocolFailure(t, err)
	denied := &fakeConn{reply: &contracts.QueryResult_AlertIncidentSummaryResponse{Data: &contracts.AlertIncidentSummaryResponse{}}}
	if _, err := service(t, denied).GetOpenIncidentCounts(context.Background()); err == nil {
		t.Fatal("unauthorized reply reported success")
	}
}

func TestGetOpenIncidentsSendsQueryAndDecodesPairedContinuation(t *testing.T) {
	first, next := uuid.New(), uuid.New()
	conn := pageConn(&contracts.AlertIncidentPageResponse{
		Status: contracts.AlertIncidentsReadinessState_Degraded, Items: []*contracts.AlertIncidentDetails{details(first)},
		Next: &contracts.AlertIncidentContinuation{RaisedSequenceNumber: 7, IncidentId: guid(next)},
	})
	after := uuid.New()
	page, err := service(t, conn).GetOpenIncidents(context.Background(), alerts.Query{Observer: "obs", Condition: "c", MinimumSeverity: alerts.Critical, Limit: 5, After: &alerts.Continuation{RaisedSequenceNumber: 4, IncidentID: after}})
	if err != nil {
		t.Fatal(err)
	}
	want := &contracts.GetOpenIncidentsRequest{EventStore: "store", Namespace: "tenant", ObserverId: "obs", Condition: "c", MinimumSeverity: contracts.AlertSeverity_Critical, Limit: 5, AfterRaisedSequenceNumber: 4, AfterIncidentId: guid(after)}
	if !proto.Equal(conn.request, want) {
		t.Fatalf("request = %v", conn.request)
	}
	if page.Readiness != alerts.Degraded || len(page.Incidents) != 1 || page.Next == nil || page.Next.IncidentID != next || page.Next.RaisedSequenceNumber != 7 {
		t.Fatalf("page = %+v", page)
	}
}

func TestZeroContinuationIdentityDisablesContinuation(t *testing.T) {
	conn := pageConn(&contracts.AlertIncidentPageResponse{})
	page, err := service(t, conn).GetOpenIncidents(context.Background(), alerts.Query{After: &alerts.Continuation{RaisedSequenceNumber: 99}})
	if err != nil || page.Next != nil || page.Incidents == nil {
		t.Fatalf("page = %+v, err = %v", page, err)
	}
	want := &contracts.GetOpenIncidentsRequest{EventStore: "store", Namespace: "tenant"}
	if !proto.Equal(conn.request, want) {
		t.Fatalf("request = %v", conn.request)
	}
}

func TestInvalidQueriesFailBeforeAnyRPC(t *testing.T) {
	queries := map[string]alerts.Query{
		"negative limit":    {Limit: -1},
		"unknown severity":  {MinimumSeverity: 5},
		"unsupported after": {After: &alerts.Continuation{RaisedSequenceNumber: math.MaxInt64 + 1, IncidentID: uuid.New()}},
	}
	for name, query := range queries {
		t.Run(name, func(t *testing.T) {
			conn := &fakeConn{}
			if _, err := service(t, conn).GetOpenIncidents(context.Background(), query); !errors.Is(err, faults.ErrInvalidConfiguration) || conn.method != "" {
				t.Fatalf("err = %v, method = %q", err, conn.method)
			}
		})
	}
}

func TestMalformedNestedRecordFailsWholePage(t *testing.T) {
	broken := details(uuid.New())
	broken.Evidence.FirstFailure = nil
	conn := pageConn(&contracts.AlertIncidentPageResponse{Items: []*contracts.AlertIncidentDetails{details(uuid.New()), broken}})
	_, err := service(t, conn).GetOpenIncidents(context.Background(), alerts.Query{})
	requireProtocolFailure(t, err)
}

func TestPageRejectsOutOfScopeItemWithoutReturningPartialResults(t *testing.T) {
	for name, target := range map[string]*contracts.AlertTarget{
		"store":     {EventStore: "Store", Namespace: "tenant"},
		"namespace": {EventStore: "store", Namespace: "tenant "},
	} {
		t.Run(name, func(t *testing.T) {
			row := details(uuid.New())
			row.Target = target
			conn := pageConn(&contracts.AlertIncidentPageResponse{
				Status: contracts.AlertIncidentsReadinessState_Ready, Items: []*contracts.AlertIncidentDetails{details(uuid.New()), row},
			})
			page, err := service(t, conn).GetOpenIncidents(t.Context(), alerts.Query{})
			requireProtocolFailure(t, err)
			if page.Incidents != nil || page.Next != nil || page.Readiness != (alerts.Page{}).Readiness {
				t.Fatalf("page = %+v", page)
			}
		})
	}
}

func TestCountsRejectOutOfScopeBucketWithoutReturningPartialResults(t *testing.T) {
	data := countsOf(&contracts.AlertIncidentCountDetails{Namespace: "tenant", Count: 2}, &contracts.AlertIncidentCountDetails{Namespace: "Tenant", Count: 3})
	data.Status = contracts.AlertIncidentsReadinessState_Ready
	counts, err := service(t, summaryConn(data)).GetOpenIncidentCounts(t.Context())
	requireProtocolFailure(t, err)
	if counts.Buckets != nil || counts.Readiness != (alerts.Counts{}).Readiness {
		t.Fatalf("counts = %+v", counts)
	}
}

func TestCountsPreserveUnknownSeverityAndRejectNegative(t *testing.T) {
	ok := summaryConn(countsOf(&contracts.AlertIncidentCountDetails{Namespace: "tenant", Condition: "c", Severity: 42, Count: 2}))
	counts, err := service(t, ok).GetOpenIncidentCounts(context.Background())
	if err != nil || len(counts.Buckets) != 1 || counts.Buckets[0].Severity != 42 || counts.Buckets[0].Count != 2 {
		t.Fatalf("counts = %+v, err = %v", counts, err)
	}
	bad := summaryConn(countsOf(&contracts.AlertIncidentCountDetails{Namespace: "tenant", Count: -1}))
	_, err = service(t, bad).GetOpenIncidentCounts(context.Background())
	requireProtocolFailure(t, err)
}

func TestSubscribeIsUnsupportedWithoutRPC(t *testing.T) {
	conn := &fakeConn{}
	if err := service(t, conn).Subscribe(context.Background(), nil); !errors.Is(err, faults.ErrUnsupported) || conn.method != "" {
		t.Fatalf("err = %v", err)
	}
}
