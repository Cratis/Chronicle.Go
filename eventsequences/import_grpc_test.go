// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"bytes"
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"

	"github.com/cratis/chronicle.go/contracts/imports"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type importsServer struct {
	imports.UnimplementedImportsServer
	mu              sync.Mutex
	id              ImportID
	begun           *imports.BeginImportRequest
	chunks          []*imports.AddImportChunkRequest
	complete        *imports.CompleteImportRequest
	rejectionReason string
}

func (s *importsServer) BeginImport(_ context.Context, request *imports.BeginImportRequest) (*imports.CommandResult_BeginImportResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.begun = request
	return &imports.CommandResult_BeginImportResponse{IsAuthorized: true, Response: &imports.BeginImportResponse{ImportId: request.ImportId, MaxChunkEvents: 2}}, nil
}

func (s *importsServer) AddImportChunk(_ context.Context, request *imports.AddImportChunkRequest) (*imports.CommandResult_AddImportChunkResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks = append(s.chunks, request)
	return &imports.CommandResult_AddImportChunkResponse{IsAuthorized: true, Response: &imports.AddImportChunkResponse{ChunkIndex: request.ChunkIndex}}, nil
}

func (s *importsServer) CompleteImport(_ context.Context, request *imports.CompleteImportRequest) (*imports.CommandResult_AbandonImportResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.complete = request
	return &imports.CommandResult_AbandonImportResponse{IsAuthorized: true, Response: &imports.AbandonImportResponse{State: imports.ImportState_Validating}}, nil
}

func (s *importsServer) progress() *imports.ImportProgressResponse {
	if s.rejectionReason != "" {
		return &imports.ImportProgressResponse{ImportId: wire.Guid(metadata.CorrelationID(s.id)), EventSequenceId: "event-log", State: imports.ImportState_Failed,
			EventsStaged: 1, RejectedCount: 1, FailureReason: "invalid source", Rejections: []*imports.ImportEventRejection{{Index: 0, EventType: "imported+2", Reasons: []string{s.rejectionReason}}}}
	}
	return &imports.ImportProgressResponse{ImportId: wire.Guid(metadata.CorrelationID(s.id)), EventSequenceId: "event-log", State: imports.ImportState_Completed,
		EventsStaged: 3, EventsCommitted: 3, FirstSequenceNumber: 0, LastSequenceNumber: 2,
		Rejections: []*imports.ImportEventRejection{{Index: 1, EventType: "imported+1", Reasons: []string{"late"}}}}
}

func (s *importsServer) GetImport(context.Context, *imports.GetImportRequest) (*imports.QueryResult_ImportProgressResponse, error) {
	return &imports.QueryResult_ImportProgressResponse{IsAuthorized: true, Data: s.progress()}, nil
}

func (s *importsServer) ObserveImport(_ *imports.ObserveImportRequest, stream grpc.ServerStreamingServer[imports.QueryResult_ImportProgressResponse]) error {
	staging := s.progress()
	staging.State, staging.EventsCommitted = imports.ImportState_Staging, 0
	for _, data := range []*imports.ImportProgressResponse{staging, s.progress()} {
		if err := stream.Send(&imports.QueryResult_ImportProgressResponse{IsAuthorized: true, Data: data}); err != nil {
			return err
		}
	}
	return nil
}

func grpcImportFixture(t *testing.T, register bool) (*Sequence, *importsServer) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	fake := &importsServer{}
	fake.id, _ = NewImportID()
	if register {
		imports.RegisterImportsServer(server, fake)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	definition, err := events.Define[imported](events.WithID("imported"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := New("store", "Default", "event-log", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	return sequence, fake
}

func TestImportOverTheImportsService(t *testing.T) {
	sequence, server := grpcImportFixture(t, true)
	correlation, _ := metadata.NewCorrelationID()
	zero := events.SequenceNumber(0)
	values := []Entry{
		{Source: "a", EventSource: "Billing", Route: Route{SourceType: "Explicit", StreamType: "changes", StreamID: "west"}, Event: imported{Name: "1"}, CorrelationID: correlation, SourceSequenceNumber: &zero, IdempotencyKey: "k1", Generation: 2},
		{Source: "a", Event: imported{Name: "2"}},
		{Source: "b", Event: imported{Name: "3"}},
	}
	result, err := sequence.Import(t.Context(), ImportEntries(values), WithImportID(server.id), WithOnInvalid(SkipInvalidEvents))
	if err != nil {
		t.Fatal(err)
	}
	assertSuccessfulImport(t, result, server.id)
	session, err := sequence.ReattachImport(t.Context(), server.id)
	if err != nil {
		t.Fatal(err)
	}
	var states []ImportState
	if err := session.ObserveProgress(t.Context(), func(progress ImportProgress) { states = append(states, progress.State) }); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(states, []ImportState{ImportStaging, ImportCompleted}) {
		t.Fatalf("progress states = %v", states)
	}
	assertImportChunkRequests(t, server)
	assertImportedSourceEvent(t, server.chunks[0].Events[0], correlation)
}

func assertSuccessfulImport(t *testing.T, result ImportResult, id ImportID) {
	t.Helper()
	if !result.IsSuccess() {
		t.Fatalf("result = %+v", result)
	}
	if result.ImportID != id {
		t.Fatal(result.ImportID)
	}
	if result.FirstSequenceNumber != 0 || result.LastSequenceNumber != 2 {
		t.Fatalf("positions = %+v", result)
	}
	if len(result.Rejections) != 1 {
		t.Fatal(result.Rejections)
	}
	if !slices.Equal(result.Rejections[0].Reasons, []string{"late"}) {
		t.Fatal(result.Rejections)
	}
}

func assertImportChunkRequests(t *testing.T, server *importsServer) {
	t.Helper()
	if ImportID(wire.Correlation(server.begun.ImportId)) != server.id {
		t.Fatal(server.begun)
	}
	if server.begun.OnInvalid != imports.OnInvalidEvent_Skip || server.begun.EventSequenceId != "event-log" {
		t.Fatal(server.begun)
	}
	if len(server.chunks) != 2 {
		t.Fatal(server.chunks)
	}
	if server.chunks[1].FirstEventIndex != 2 {
		t.Fatal(server.chunks[1])
	}
	if server.complete.ExpectedChunks != 2 || server.complete.ExpectedEvents != 3 {
		t.Fatal(server.complete)
	}
}

func assertImportedSourceEvent(t *testing.T, first *imports.ImportEvent, correlation metadata.CorrelationID) {
	t.Helper()
	if wire.Correlation(first.CorrelationId) != correlation {
		t.Fatal(first.CorrelationId)
	}
	if first.EventType.Generation != 2 {
		t.Fatal(first.EventType)
	}
	for field, values := range map[string][2]string{
		"idempotency": {first.IdempotencyKey, "k1"}, "source ID": {first.EventSourceId, "a"}, "source": {first.EventSource, "Billing"},
		"type": {first.EventSourceType, "Explicit"}, "stream type": {first.EventStreamType, "changes"}, "stream ID": {first.EventStreamId, "west"},
	} {
		if values[0] != values[1] {
			t.Errorf("%s = %q, want %q", field, values[0], values[1])
		}
	}
}

func TestImportEventCarriesASourceSequenceNumberOfZero(t *testing.T) {
	zero, five := uint64(0), uint64(5)
	encoded := func(number *uint64) []byte {
		event, err := importEventContract(&importEvent{eventType: nil, sourceSequenceNumber: number})
		if err != nil {
			t.Fatal(err)
		}
		data, err := proto.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	field16 := []byte{0x80, 0x01}
	if data := encoded(&zero); !bytes.Contains(data, append(field16, 0x00)) {
		t.Fatalf("zero not present: % x", data)
	}
	if data := encoded(&five); !bytes.Contains(data, append(field16, 0x05)) || bytes.Count(data, field16) != 1 {
		t.Fatalf("five: % x", data)
	}
	if data := encoded(nil); bytes.Contains(data, field16) {
		t.Fatalf("absent number encoded: % x", data)
	}
}

func TestImportsServiceMissingIsUnsupported(t *testing.T) {
	sequence, _ := grpcImportFixture(t, false)
	if _, err := sequence.BeginImport(t.Context()); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("err = %v", err)
	}
}
