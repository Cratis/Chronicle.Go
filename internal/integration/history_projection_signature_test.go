//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

func TestHistoryProjectionSkipRequiresExactKernelFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*historyProjectionEvidence)
	}{
		{"missing model", func(e *historyProjectionEvidence) { e.last.Exists = false }},
		{"missing watermark", func(e *historyProjectionEvidence) { e.last.LastHandled = nil }},
		{"advanced watermark", func(e *historyProjectionEvidence) { *e.last.LastHandled = 1 }},
		{"wrong source model", func(e *historyProjectionEvidence) { e.last.Value.ID = uuid.Nil }},
		{"wrong model value", func(e *historyProjectionEvidence) { e.last.Value.Name = "before" }},
		{"wrong constant", func(e *historyProjectionEvidence) { e.last.Value.Number = 0 }},
		{"wrong state", func(e *historyProjectionEvidence) { e.last.Value.State = "other" }},
		{"wrong enabled", func(e *historyProjectionEvidence) { e.last.Value.Enabled = false }},
		{"wrong product", func(e *historyProjectionEvidence) { e.last.Value.ProductName = "other" }},
		{"wrong local", func(e *historyProjectionEvidence) { e.last.Value.Local = "other" }},
		{"wrong excluded", func(e *historyProjectionEvidence) { e.last.Value.Excluded = "other" }},
		{"wrong occurrence", func(e *historyProjectionEvidence) { e.last.Value.Occurred = time.Now() }},
		{"missing note", func(e *historyProjectionEvidence) { e.last.Value.Note = nil }},
		{"wrong note", func(e *historyProjectionEvidence) { *e.last.Value.Note = "different" }},
		{"no callback", func(e *historyProjectionEvidence) { e.observed = nil }},
		{"duplicate callback", func(e *historyProjectionEvidence) { e.observed = append(e.observed, e.observed[0]) }},
		{"wrong callback value", func(e *historyProjectionEvidence) { e.observed[0].value = "revised" }},
		{"replay callback", func(e *historyProjectionEvidence) { e.observed[0].state = events.ObservationReplay }},
		{"missing history", func(e *historyProjectionEvidence) { e.history = e.history[:1] }},
		{"missing observers", func(e *historyProjectionEvidence) { e.observers = e.observers[:3] }},
		{"missing failure responses", func(e *historyProjectionEvidence) { e.failures = e.failures[:3] }},
		{"invalid opening", func(e *historyProjectionEvidence) { e.history[0].Content = []byte(`{"fullName":"different"}`) }},
		{"invalid rename", func(e *historyProjectionEvidence) { e.history[1].Content = []byte(`{"name":"different"}`) }},
		{"wrong JSON casing", func(e *historyProjectionEvidence) { e.history[1].Content = []byte(`{"Name":"before"}`) }},
		{"extra rename property", func(e *historyProjectionEvidence) { e.history[1].Content = []byte(`{"name":"before","other":""}`) }},
		{"null rename", func(e *historyProjectionEvidence) { e.history[1].Content = []byte(`null`) }},
		{"malformed rename", func(e *historyProjectionEvidence) { e.history[1].Content = []byte(`invalid`) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, source := historyProjectionSignatureFixture()
			tc.change(&e)
			if strandedInitialHistoryProjection(context.DeadlineExceeded, source, "projection", e) {
				t.Fatal("must not skip a different failure")
			}
		})
	}
	t.Run("only a poll deadline qualifies", func(t *testing.T) {
		e, source := historyProjectionSignatureFixture()
		if !strandedInitialHistoryProjection(context.DeadlineExceeded, source, "projection", e) {
			t.Fatal("reproduced signature not recognized")
		}
		for _, err := range []error{nil, context.Canceled, errors.New("transport failed")} {
			if strandedInitialHistoryProjection(err, source, "projection", e) {
				t.Fatalf("must not skip error %v", err)
			}
		}
	})
	for i, observer := range []string{"projection", "statistics", "global statistics", "reactor"} {
		for _, tc := range []struct {
			name   string
			change func(*historyProjectionEvidence, int)
		}{
			{"missing", func(e *historyProjectionEvidence, i int) { e.observers[i] = nil }},
			{"wrong id", func(e *historyProjectionEvidence, i int) { e.observers[i].Id = "other" }},
			{"wrong sequence", func(e *historyProjectionEvidence, i int) { e.observers[i].EventSequenceId = "system" }},
			{"wrong type", func(e *historyProjectionEvidence, i int) { e.observers[i].Type = contracts.ObserverType_Reducer }},
			{"wrong owner", func(e *historyProjectionEvidence, i int) { e.observers[i].Owner = contracts.ObserverOwner(99) }},
			{"inactive", func(e *historyProjectionEvidence, i int) {
				e.observers[i].RunningState = contracts.ObserverRunningState_Suspended
			}},
			{"disconnected", func(e *historyProjectionEvidence, i int) { e.observers[i].IsSubscribed = false }},
			{"different last", func(e *historyProjectionEvidence, i int) { e.observers[i].LastHandledEventSequenceNumber++ }},
			{"different next", func(e *historyProjectionEvidence, i int) { e.observers[i].NextEventSequenceNumber++ }},
			{"different tail", func(e *historyProjectionEvidence, i int) { e.observers[i].TailEventSequenceNumber++ }},
			{"different count", func(e *historyProjectionEvidence, i int) { e.observers[i].HandledEventCount++ }},
			{"missing failures", func(e *historyProjectionEvidence, i int) { e.failures[i] = nil }},
			{"failed partition", func(e *historyProjectionEvidence, i int) {
				e.failures[i].Items = []*contracts.FailedPartition{{Partition: "source"}}
			}},
		} {
			t.Run(observer+"/"+tc.name, func(t *testing.T) {
				e, source := historyProjectionSignatureFixture()
				tc.change(&e, i)
				if strandedInitialHistoryProjection(context.DeadlineExceeded, source, "projection", e) {
					t.Fatal("must not skip a different observer state")
				}
			})
		}
	}
	for i, event := range []string{"opened", "renamed"} {
		for _, tc := range []struct {
			name   string
			change func(*events.Appended)
		}{
			{"wrong source", func(e *events.Appended) { e.Context.SourceID = "other" }},
			{"wrong position", func(e *events.Appended) { e.Context.SequenceNumber++ }},
			{"wrong type", func(e *events.Appended) { e.Context.EventType.ID = "other" }},
			{"wrong generation", func(e *events.Appended) { e.Context.EventType.Generation++ }},
			{"already revised", func(e *events.Appended) { e.Revisions = []events.Revision{{}} }},
		} {
			t.Run(event+"/"+tc.name, func(t *testing.T) {
				e, source := historyProjectionSignatureFixture()
				tc.change(&e.history[i])
				if strandedInitialHistoryProjection(context.DeadlineExceeded, source, "projection", e) {
					t.Fatal("must not skip different persisted history")
				}
			})
		}
	}
}

func historyProjectionSignatureFixture() (historyProjectionEvidence, events.SourceID) {
	id := uuid.MustParse("9094f708-d2c2-4f4f-bbde-8cf50ba182ae")
	source := events.SourceID(id.String())
	watermark := events.SequenceNumber(0)
	note := "present"
	e := historyProjectionEvidence{
		last:     readmodels.Instance[ProjectionAccount]{Exists: true, LastHandled: &watermark, Value: ProjectionAccount{ID: id, Name: "original", Note: &note, State: "active", Number: 42, Enabled: true}},
		observed: []historyObservation{{"before", events.ObservationInitial}},
	}
	for i, id := range []string{"projection", "$system.statistics.event-types", "$system.statistics.event-types.global", "history-observer"} {
		info := &contracts.ObserverInformation{
			Id: id, Type: contracts.ObserverType_Projection, Owner: contracts.ObserverOwner_Kernel, EventSequenceId: "event-log",
			RunningState: contracts.ObserverRunningState_Active, IsSubscribed: true,
			LastHandledEventSequenceNumber: 0, NextEventSequenceNumber: 1, TailEventSequenceNumber: 1, HandledEventCount: 1,
		}
		if i == 3 {
			info.Type, info.Owner = contracts.ObserverType_Reactor, contracts.ObserverOwner_Client
			info.LastHandledEventSequenceNumber, info.NextEventSequenceNumber = 1, 2
		}
		e.observers = append(e.observers, info)
		e.failures = append(e.failures, &contracts.IEnumerable_FailedPartition{})
	}
	for i, id := range []events.TypeID{"ProjectionAccountOpened", "ProjectionAccountRenamed"} {
		e.history = append(e.history, events.Appended{Context: events.Context{SourceID: source, SequenceNumber: events.SequenceNumber(i), EventType: events.TypeRef{ID: id, Generation: 1}}})
	}
	e.history[0].Content = []byte(`{"fullName":"original","productName":"","local":"","excluded":""}`)
	e.history[1].Content = []byte(`{"name":"before"}`)
	return e, source
}
