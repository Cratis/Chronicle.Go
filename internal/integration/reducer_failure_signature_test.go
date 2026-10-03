//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestReducerDeletionSkipRequiresExactKernelFailure(t *testing.T) {
	type evidence struct {
		err       error
		last      readmodels.Instance[ReducedBalance]
		deletions uint32
		info      *contracts.ObserverInformation
		failures  *contracts.IEnumerable_FailedPartition
		history   []events.Appended
	}
	for _, tc := range []struct {
		name   string
		change func(*evidence)
		want   bool
	}{
		{name: "reproduced signature", change: func(*evidence) {}, want: true},
		{name: "successful poll", change: func(e *evidence) { e.err = nil }},
		{name: "read error", change: func(e *evidence) { e.err = errors.New("transport failed") }},
		{name: "canceled poll", change: func(e *evidence) { e.err = context.Canceled }},
		{name: "deletion delivered", change: func(e *evidence) { e.deletions = 1 }},
		{name: "absent model", change: func(e *evidence) { e.last.Exists = false }},
		{name: "wrong value", change: func(e *evidence) { e.last.Value.Amount = -10 }},
		{name: "wrong key", change: func(e *evidence) { e.last.Value.ID = "other" }},
		{name: "missing watermark", change: func(e *evidence) { e.last.LastHandled = nil }},
		{name: "newer watermark", change: func(e *evidence) { *e.last.LastHandled = 2 }},
		{name: "missing observer", change: func(e *evidence) { e.info = nil }},
		{name: "wrong observer", change: func(e *evidence) { e.info.Id = "other" }},
		{name: "wrong event sequence", change: func(e *evidence) { e.info.EventSequenceId = "other" }},
		{name: "wrong owner", change: func(e *evidence) { e.info.Owner = contracts.ObserverOwner_Kernel }},
		{name: "wrong observer type", change: func(e *evidence) { e.info.Type = contracts.ObserverType_Reactor }},
		{name: "inactive observer", change: func(e *evidence) { e.info.RunningState = contracts.ObserverRunningState_Suspended }},
		{name: "disconnected observer", change: func(e *evidence) { e.info.IsSubscribed = false }},
		{name: "advanced checkpoint", change: func(e *evidence) { e.info.LastHandledEventSequenceNumber = 2 }},
		{name: "advanced next", change: func(e *evidence) { e.info.NextEventSequenceNumber = 3 }},
		{name: "different tail", change: func(e *evidence) { e.info.TailEventSequenceNumber = 3 }},
		{name: "different count", change: func(e *evidence) { e.info.HandledEventCount = 3 }},
		{name: "missing failure response", change: func(e *evidence) { e.failures = nil }},
		{name: "failed partition", change: func(e *evidence) { e.failures.Items = []*contracts.FailedPartition{{Partition: "balance"}} }},
		{name: "missing deletion", change: func(e *evidence) { e.history = e.history[:2] }},
		{name: "wrong event type", change: func(e *evidence) { e.history[2].Context.EventType.ID = "Other" }},
		{name: "wrong generation", change: func(e *evidence) { e.history[2].Context.EventType.Generation = 2 }},
		{name: "wrong source", change: func(e *evidence) { e.history[2].Context.SourceID = "other" }},
		{name: "wrong sequence", change: func(e *evidence) { e.history[2].Context.SequenceNumber = 3 }},
		{name: "invalid deletion content", change: func(e *evidence) { e.history[2].Content = []byte("invalid") }},
		{name: "null deletion content", change: func(e *evidence) { e.history[2].Content = []byte("null") }},
		{name: "unexpected deletion content", change: func(e *evidence) { e.history[2].Content = []byte(`{"unexpected":true}`) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			watermark := events.SequenceNumber(1)
			e := evidence{
				err: context.DeadlineExceeded,
				last: readmodels.Instance[ReducedBalance]{
					Value: ReducedBalance{ID: "balance", Amount: -6}, Exists: true, LastHandled: &watermark,
				},
				info: &contracts.ObserverInformation{
					Id: "go-balance", Type: contracts.ObserverType_Reducer, EventSequenceId: "event-log", Owner: contracts.ObserverOwner_Client,
					RunningState: contracts.ObserverRunningState_Active, IsSubscribed: true,
					LastHandledEventSequenceNumber: 1, NextEventSequenceNumber: 2, TailEventSequenceNumber: 2, HandledEventCount: 2,
				},
				failures: &contracts.IEnumerable_FailedPartition{},
			}
			for i, id := range []events.TypeID{"ReducedAmountChanged", "ReducedAmountChanged", "ReducedAccountDeleted"} {
				e.history = append(e.history, events.Appended{
					Context: events.Context{SequenceNumber: events.SequenceNumber(i), SourceID: "balance", EventType: events.TypeRef{ID: id, Generation: 1}},
					Content: []byte("{}"),
				})
			}
			tc.change(&e)
			if got := strandedReducerDeletion(e.err, e.last, e.deletions, e.info, e.failures, e.history); got != tc.want {
				t.Fatalf("skip = %v, want %v", got, tc.want)
			}
		})
	}
}
