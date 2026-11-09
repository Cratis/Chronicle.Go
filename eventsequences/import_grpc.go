// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

//go:build chronicle_imports

package eventsequences

import (
	"context"
	"errors"
	"io"
	"maps"

	"github.com/cratis/chronicle.go/contracts/bcl"
	"github.com/cratis/chronicle.go/contracts/imports"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/protobuf/proto"
)

// grpcImports binds the kernel's Imports service. It needs contracts/imports,
// which the pinned contracts do not carry yet; see import_unsupported.go.
type grpcImports struct {
	client                     imports.ImportsClient
	store, namespace, sequence string
}

func newImportTransport(s *Sequence) importTransport {
	return grpcImports{client: imports.NewImportsClient(s.conn), store: string(s.store), namespace: string(s.namespace), sequence: string(s.id)}
}

func importGuid(id ImportID) *bcl.Guid { return wire.Guid(metadata.CorrelationID(id)) }

func (g grpcImports) begin(ctx context.Context, id ImportID, onInvalid OnInvalidEvent) (importOpened, error) {
	request := &imports.BeginImportRequest{EventStore: g.store, Namespace: g.namespace, EventSequenceId: g.sequence, OnInvalid: imports.OnInvalidEvent(onInvalid)}
	if !id.IsZero() {
		request.ImportId = importGuid(id)
	}
	envelope, err := g.client.BeginImport(ctx, request)
	if err = commandEnvelope(envelope, err); err != nil {
		return importOpened{}, err
	}
	response := envelope.Response
	return importOpened{id: ImportID(wire.Correlation(response.ImportId)), maxChunkEvents: response.MaxChunkEvents, maxChunkBytes: response.MaxChunkBytes, err: response.Error}, nil
}

func (g grpcImports) addChunk(ctx context.Context, chunk importChunk) (importChunkAck, error) {
	request := &imports.AddImportChunkRequest{EventStore: g.store, Namespace: g.namespace, ImportId: importGuid(chunk.id), ChunkIndex: chunk.index, FirstEventIndex: chunk.firstEventIndex}
	for i := range chunk.events {
		event, err := importEventContract(&chunk.events[i])
		if err != nil {
			return importChunkAck{}, err
		}
		request.Events = append(request.Events, event)
	}
	envelope, err := g.client.AddImportChunk(ctx, request)
	if err = commandEnvelope(envelope, err); err != nil {
		return importChunkAck{}, err
	}
	return importChunkAck{index: envelope.Response.ChunkIndex, err: envelope.Response.Error}, nil
}

func (g grpcImports) complete(ctx context.Context, id ImportID, expectedChunks int32, expectedEvents int64) (importStatus, error) {
	envelope, err := g.client.CompleteImport(ctx, &imports.CompleteImportRequest{EventStore: g.store, Namespace: g.namespace, ImportId: importGuid(id),
		ExpectedChunks: expectedChunks, ExpectedEvents: expectedEvents})
	if err = commandEnvelope(envelope, err); err != nil {
		return importStatus{}, err
	}
	return importStatus{state: ImportState(envelope.Response.State), err: envelope.Response.Error}, nil
}

func (g grpcImports) abandon(ctx context.Context, id ImportID) (importStatus, error) {
	envelope, err := g.client.AbandonImport(ctx, &imports.AbandonImportRequest{EventStore: g.store, Namespace: g.namespace, ImportId: importGuid(id)})
	if err = commandEnvelope(envelope, err); err != nil {
		return importStatus{}, err
	}
	return importStatus{state: ImportState(envelope.Response.State), err: envelope.Response.Error}, nil
}

func (g grpcImports) get(ctx context.Context, id ImportID) (importSnapshot, bool, error) {
	result, err := g.client.GetImport(ctx, &imports.GetImportRequest{EventStore: g.store, Namespace: g.namespace, ImportId: importGuid(id)})
	if err != nil {
		return importSnapshot{}, false, err
	}
	if err = wire.CheckEnvelope(result); err != nil {
		return importSnapshot{}, false, err
	}
	if result.Data == nil {
		return importSnapshot{}, false, nil
	}
	return importSnapshotOf(result.Data), true, nil
}

func (g grpcImports) observe(ctx context.Context, id ImportID, yield func(importSnapshot) bool) error {
	stream, err := g.client.ObserveImport(ctx, &imports.ObserveImportRequest{EventStore: g.store, Namespace: g.namespace, ImportId: importGuid(id)})
	if err != nil {
		return err
	}
	for {
		result, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err = wire.CheckEnvelope(result); err != nil {
			return err
		}
		if result.Data != nil && !yield(importSnapshotOf(result.Data)) {
			return nil
		}
	}
}

func commandEnvelope(envelope proto.Message, err error) error {
	if err != nil {
		return err
	}
	if err = wire.CheckEnvelope(envelope); err != nil {
		return err
	}
	return wire.RequireMessage(envelope, "Response")
}

func importEventContract(event *importEvent) (*imports.ImportEvent, error) {
	result := &imports.ImportEvent{EventSourceId: event.sourceID, EventSourceType: event.sourceType, EventStreamType: event.streamType, EventStreamId: event.streamID,
		EventType: &imports.EventType{Id: event.eventType.GetId(), Generation: event.eventType.GetGeneration()}, Content: event.content, Tags: event.tags,
		Subject: event.subject, CorrelationId: wire.Guid(event.correlation), IdempotencyKey: event.idempotencyKey, CausedBy: importIdentity(event.causedBy)}
	if event.occurred != "" {
		result.Occurred = &imports.SerializableDateTimeOffset{Value: event.occurred}
	}
	for _, tag := range event.namedTags {
		result.NamedTags = append(result.NamedTags, &imports.NamedTag{Name: tag.GetName(), Value: tag.GetValue()})
	}
	for _, cause := range event.causation {
		result.Causation = append(result.Causation, &imports.Causation{Occurred: &imports.SerializableDateTimeOffset{Value: cause.GetOccurred().GetValue()},
			Type: cause.GetType(), Properties: maps.Clone(cause.GetProperties())})
	}
	if event.sourceSequenceNumber != nil {
		result.SourceSequenceNumber = *event.sourceSequenceNumber
		if result.SourceSequenceNumber == 0 {
			// The kernel reads this field as nullable; an omitted zero would arrive as null.
			if err := wire.PresentZero(result, "SourceSequenceNumber"); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func importIdentity(identity *sequences.Identity) *imports.Identity {
	if identity == nil {
		return nil
	}
	return &imports.Identity{Subject: identity.Subject, Name: identity.Name, UserName: identity.UserName, OnBehalfOf: importIdentity(identity.OnBehalfOf)}
}

func importSnapshotOf(data *imports.ImportProgressResponse) importSnapshot {
	result := ImportResult{ImportID: ImportID(wire.Correlation(data.ImportId)), State: ImportState(data.State), EventsStaged: data.EventsStaged,
		EventsCommitted: data.EventsCommitted, AlreadyAppended: data.AlreadyAppended, RejectedCount: data.RejectedCount, FailureReason: data.FailureReason,
		FirstSequenceNumber: events.Unavailable, LastSequenceNumber: events.Unavailable}
	// proto3 cannot tell a first position of zero from none, so a committed count decides.
	if data.EventsCommitted > 0 {
		result.FirstSequenceNumber, result.LastSequenceNumber = events.SequenceNumber(data.FirstSequenceNumber), events.SequenceNumber(data.LastSequenceNumber)
	}
	for _, rejection := range data.Rejections {
		result.Rejections = append(result.Rejections, ImportRejection{Index: rejection.Index, EventType: rejection.EventType, Reasons: append([]string(nil), rejection.Reasons...)})
	}
	return importSnapshot{result: result, sequence: data.EventSequenceId, onInvalid: OnInvalidEvent(data.OnInvalid), eventsReceived: data.EventsReceived,
		maxChunkEvents: data.MaxChunkEvents, maxChunkBytes: data.MaxChunkBytes, acknowledgedChunks: data.AcknowledgedChunks, acknowledgedEvents: data.AcknowledgedEvents}
}
