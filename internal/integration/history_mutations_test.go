//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
)

func awaitHistoryMutation(t *testing.T, parent context.Context, sequence *eventsequences.Sequence, source events.SourceID, ready func([]events.Appended) bool) []events.Appended {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		history, err := sequence.ReadSource(ctx, source, eventsequences.SourceFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if ready(history) {
			return history
		}
		select {
		case <-ctx.Done():
			t.Fatalf("history mutation not applied: %+v: %v", history, ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestKernelRedactOnePreservesMarkerAndAudit(t *testing.T) {
	ctx, sequence := batchFixture(t)
	original, err := sequence.Append(ctx, "source", BatchOpened{"secret"})
	if err != nil || original.Err() != nil {
		t.Fatalf("%+v %v", original, err)
	}
	mutation := metadata.WithIdentity(ctx, identities.Identity{Subject: "history-operator", Name: "History Operator"})
	mutation = metadata.WithCausation(mutation, metadata.Causation{Occurred: time.Now().UTC(), Type: "approved-removal", Properties: map[string]string{"ticket": "40"}})
	defer sequence.OnAppend(func(eventsequences.AppendNotification) { t.Error("redaction emitted append notification") })()
	if err := sequence.Redact(mutation, *original.Position, "approved removal"); err != nil {
		t.Fatal(err)
	}
	history := awaitHistoryMutation(t, ctx, sequence, "source", func(h []events.Appended) bool {
		return len(h) == 1 && h[0].Context.EventType.ID == events.RedactedTypeID
	})
	marker, err := history[0].Redaction()
	if err != nil || marker.Reason != "approved removal" || marker.OriginalEventType != "batch-opened" || marker.CorrelationID.String() != original.CorrelationID.String() {
		t.Fatalf("%+v %v", marker, err)
	}
	if history[0].Context.SequenceNumber != *original.Position || history[0].Context.CausedBy.Subject != "history-operator" || len(history[0].Revisions) != 0 || strings.Contains(string(history[0].Content), "secret") || strings.Contains(string(history[0].OriginalContent), "secret") {
		t.Fatalf("redacted history: %+v", history[0])
	}
}

func TestKernelRedactSourceHonorsTypeFilterAndIsolation(t *testing.T) {
	ctx, sequence := batchFixture(t)
	for _, source := range []events.SourceID{"target", "other"} {
		r, err := sequence.AppendMany(ctx, source, []any{BatchOpened{"opened"}, BatchChanged{"changed"}})
		if err != nil || r.Err() != nil {
			t.Fatalf("%+v %v", r, err)
		}
	}
	if err := sequence.RedactForEventSource(ctx, "target", "remove opened", "batch-opened"); err != nil {
		t.Fatal(err)
	}
	awaitHistoryMutation(t, ctx, sequence, "target", func(h []events.Appended) bool {
		return len(h) == 2 && h[0].Context.EventType.ID == events.RedactedTypeID && h[1].Context.EventType.ID == "batch-changed"
	})
	if err := sequence.RedactForEventSource(ctx, "target", "remove remainder"); err != nil {
		t.Fatal(err)
	}
	awaitHistoryMutation(t, ctx, sequence, "target", func(h []events.Appended) bool {
		return len(h) == 2 && h[0].Context.EventType.ID == events.RedactedTypeID && h[1].Context.EventType.ID == events.RedactedTypeID
	})
	other, err := sequence.ReadSource(ctx, "other", eventsequences.SourceFilter{})
	if err != nil || len(other) != 2 || other[0].Context.EventType.ID != "batch-opened" || other[1].Context.EventType.ID != "batch-changed" {
		t.Fatalf("source isolation: %+v %v", other, err)
	}
}

func TestKernelRevisionPreservesOriginalAndRevisionAudit(t *testing.T) {
	ctx, sequence := batchFixture(t)
	original, err := sequence.Append(ctx, "source", BatchOpened{"before"})
	if err != nil || original.Err() != nil {
		t.Fatalf("%+v %v", original, err)
	}
	mutation := metadata.WithIdentity(ctx, identities.Identity{Subject: "revision-operator"})
	defer sequence.OnAppend(func(eventsequences.AppendNotification) { t.Error("revision emitted append notification") })()
	if err := sequence.Revise(mutation, *original.Position, BatchOpened{"after"}); err != nil {
		t.Fatal(err)
	}
	history := awaitHistoryMutation(t, ctx, sequence, "source", func(h []events.Appended) bool { return len(h) == 1 && len(h[0].Revisions) == 1 })
	if !strings.Contains(string(history[0].Content), "after") || !strings.Contains(string(history[0].OriginalContent), "before") || history[0].Revisions[0].CausedBy.Subject != "revision-operator" {
		t.Fatalf("revision: %+v", history[0])
	}
	if err := sequence.Redact(ctx, *original.Position, "remove original and revisions"); err != nil {
		t.Fatal(err)
	}
	redacted := awaitHistoryMutation(t, ctx, sequence, "source", func(h []events.Appended) bool {
		return len(h) == 1 && h[0].Context.EventType.ID == events.RedactedTypeID
	})
	if len(redacted[0].Revisions) != 0 || strings.Contains(string(redacted[0].Content), "after") || strings.Contains(string(redacted[0].OriginalContent), "before") {
		t.Fatal("redaction retained payload or revisions")
	}
}

func TestKernelCompleteStreamRefusalsAndFutureAppends(t *testing.T) {
	ctx, sequence := batchFixture(t)
	route := eventsequences.Route{StreamType: "Orders", StreamID: "order-stream"}
	original, err := sequence.Append(ctx, "source", BatchOpened{"before"}, eventsequences.WithRoute(route))
	if err != nil || original.Err() != nil {
		t.Fatalf("%+v %v", original, err)
	}
	if _, err := sequence.CompleteStream(ctx, events.AllStreamTypes, events.DefaultStreamID); !errors.Is(err, eventsequences.DefaultStreamCannotBeCompleted) {
		t.Fatal(err)
	}
	tail, err := sequence.CompleteStream(ctx, route.StreamType, route.StreamID)
	if err != nil || tail != *original.Position {
		t.Fatalf("tail=%d error=%v", tail, err)
	}
	if _, err := sequence.CompleteStream(ctx, route.StreamType, route.StreamID); !errors.Is(err, eventsequences.StreamAlreadyCompleted) {
		t.Fatal(err)
	}
	rejected, err := sequence.Append(ctx, "different-source", BatchOpened{"after"}, eventsequences.WithRoute(route))
	if err != nil || rejected.Disposition != eventsequences.Rejected || len(rejected.ConstraintViolations) != 1 {
		t.Fatalf("closed append: %+v %v", rejected, err)
	}
	if _, err := sequence.CompleteStream(ctx, "", ""); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
}
