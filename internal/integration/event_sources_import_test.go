//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

func TestKernelNamedEventSourceImportPersistsAttribution(t *testing.T) {
	f, store := newHostedSourceStore(t)
	registerHostedSource(t, f, store)
	entries := []eventsequences.Entry{hostedSourceEntry("imported-source", "one"), hostedSourceEntry("imported-source", "two")}
	result, err := store.EventLog().Import(f.ctx, eventsequences.ImportEntries(entries))
	if err != nil {
		t.Fatal(err)
	}
	if !result.IsSuccess() {
		t.Fatal(result)
	}
	if result.EventsCommitted != 2 || result.RejectedCount != 0 {
		t.Fatal(result)
	}
	appended := readHostedSource(t, f, store.EventLog(), "imported-source")
	if len(appended) != 2 {
		t.Fatal(appended)
	}
	for _, event := range appended {
		assertHostedSourceContext(t, event.Context)
	}
	if result.FirstSequenceNumber != appended[0].Context.SequenceNumber || result.LastSequenceNumber != appended[1].Context.SequenceNumber {
		t.Fatal("import positions differ from persisted history", result)
	}
}

func assertHostedImportSourceRejection(t *testing.T, result eventsequences.ImportResult) {
	t.Helper()
	if result.RejectedCount != 1 || len(result.Rejections) != 1 {
		t.Fatalf("missing source rejection: %+v", result)
	}
	rejection := result.Rejections[0]
	if rejection.Index != 1 || rejection.EventType == "" {
		t.Fatal(rejection)
	}
	if len(rejection.Reasons) == 0 {
		t.Fatal("source rejection has no diagnostic reasons")
	}
	t.Logf("source import rejection: %+v", rejection)
}

func assertHostedImportInvalidMode(t *testing.T, result eventsequences.ImportResult, mode eventsequences.OnInvalidEvent) {
	t.Helper()
	if mode == eventsequences.SkipInvalidEvents {
		if !result.IsSuccess() {
			t.Fatalf("skip-invalid import failed: %+v", result)
		}
		if result.EventsCommitted != 2 {
			t.Fatal(result)
		}
		return
	}
	if result.State != eventsequences.ImportFailed {
		t.Fatalf("reject-invalid import did not fail: %+v", result)
	}
	if result.EventsCommitted != 0 {
		t.Fatal("reject-invalid import partially committed", result)
	}
}

func TestKernelNamedEventSourceImportRejectionsRespectInvalidMode(t *testing.T) {
	f, store := newHostedSourceStore(t)
	registerHostedSource(t, f, store)
	for name, invalid := range invalidHostedSourceEntries() {
		for modeName, mode := range map[string]eventsequences.OnInvalidEvent{"reject": eventsequences.RejectInvalidEvents, "skip": eventsequences.SkipInvalidEvents} {
			t.Run(name+"/"+modeName, func(t *testing.T) {
				source := events.SourceID(strings.ReplaceAll(name, " ", "-") + "-" + modeName)
				entries := []eventsequences.Entry{hostedSourceEntry(source, "before"), invalid, hostedSourceEntry(source, "after")}
				result, err := store.EventLog().Import(f.ctx, eventsequences.ImportEntries(entries), eventsequences.WithOnInvalid(mode))
				if err != nil {
					t.Fatal(err)
				}
				assertHostedImportSourceRejection(t, result)
				assertHostedImportInvalidMode(t, result, mode)
				appended := readHostedSource(t, f, store.EventLog(), source)
				if int64(len(appended)) != result.EventsCommitted {
					t.Fatal("import commit count differs from persisted control events", result, appended)
				}
				assertHostedInvalidImportLeftNoEvents(t, f, store.EventLog(), invalid.Source)
			})
		}
	}
}

func assertHostedInvalidImportLeftNoEvents(t *testing.T, f *kernelFixture, sequence *eventsequences.Sequence, source events.SourceID) {
	t.Helper()
	appended, err := sequence.ReadSource(f.ctx, source, eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(appended) != 0 {
		t.Fatal("invalid import event was persisted", appended)
	}
}
