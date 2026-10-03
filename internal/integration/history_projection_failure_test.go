//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
)

type historyProjectionEvidence struct {
	last      readmodels.Instance[ProjectionAccount]
	observed  []historyObservation
	history   []events.Appended
	observers []*contracts.ObserverInformation
	failures  []*contracts.IEnumerable_FailedPartition
}

func awaitInitialHistoryProjection(t *testing.T, f *kernelFixture, store *chronicle.EventStore, reader *readmodels.Reader[ProjectionAccount], source events.SourceID, projectionID string, observed <-chan historyObservation) {
	t.Helper()
	last, err := pollInitialHistoryProjection(f.ctx, reader, source)
	if err == nil {
		return
	}
	// Diagnose before client cleanup. Only the local poll's deadline may lead
	// to the known-kernel skip; RPC failures and parent cancellation still fail.
	if !errors.Is(err, context.DeadlineExceeded) || f.ctx.Err() != nil {
		t.Fatalf("initial history projection: %+v: %v", last, err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	evidence := historyProjectionEvidence{last: last}
	for _, id := range []string{projectionID, "$system.statistics.event-types", "$system.statistics.event-types.global", "history-observer"} {
		info, infoErr := contracts.NewObserversClient(f.conn).GetObserverInformation(ctx, &contracts.GetObserverInformationRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), EventSequenceId: "event-log", ObserverId: id})
		failures, failuresErr := contracts.NewFailedPartitionsClient(f.conn).GetFailedPartitions(ctx, &contracts.GetFailedPartitionsRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: id})
		t.Logf("initial history projection store=%s observer=%v (%v), failures=%v (%v)", f.storeName, info, infoErr, failures, failuresErr)
		if infoErr != nil || failuresErr != nil {
			t.Fatal("could not diagnose initial history projection timeout", err)
		}
		evidence.observers = append(evidence.observers, info)
		evidence.failures = append(evidence.failures, failures)
	}
	var historyErr error
	evidence.history, historyErr = store.EventLog().ReadSource(ctx, source, eventsequences.SourceFilter{})
	if historyErr != nil {
		t.Fatal("read history after initial projection timeout", historyErr)
	}
	for len(observed) > 0 {
		evidence.observed = append(evidence.observed, <-observed)
	}
	if ctx.Err() == nil && strandedInitialHistoryProjection(err, source, projectionID, evidence) {
		t.Skip("kernel shares fresh observers' catch-up partition sets and drops live projection event 1: https://github.com/Cratis/Chronicle/issues/4558")
	}
	t.Fatalf("initial history projection store=%s last=%+v observations=%+v history=%+v: %v", f.storeName, last, evidence.observed, evidence.history, err)
}

func pollInitialHistoryProjection(parent context.Context, reader *readmodels.Reader[ProjectionAccount], source events.SourceID) (readmodels.Instance[ProjectionAccount], error) {
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var last readmodels.Instance[ProjectionAccount]
	for {
		value, err := reader.Get(ctx, readmodels.Key(source))
		if err != nil {
			if ctx.Err() != nil {
				return last, ctx.Err()
			}
			return last, err
		}
		last = value
		if value.Exists && value.Value.Name == "before" {
			return value, nil
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-ticker.C:
		}
	}
}

// The 19.29.4 shared-set defect strands all three projections at 0 while the
// reactor successfully handles 1. Do not skip a lone projection failure, absent
// history, a failed/disconnected observer, or any revision/redaction timeout.
func strandedInitialHistoryProjection(err error, source events.SourceID, projectionID string, e historyProjectionEvidence) bool {
	if !errors.Is(err, context.DeadlineExceeded) || !e.last.Exists ||
		e.last.LastHandled == nil || *e.last.LastHandled != 0 ||
		e.last.Value.ID.String() != string(source) || e.last.Value.Name != "original" ||
		e.last.Value.Note == nil || *e.last.Value.Note != "present" ||
		e.last.Value.State != "active" || e.last.Value.Number != 42 || !e.last.Value.Enabled ||
		e.last.Value.ProductName != "" || e.last.Value.Local != "" || e.last.Value.Excluded != "" ||
		len(e.observed) != 1 || e.observed[0] != (historyObservation{"before", events.ObservationInitial}) ||
		len(e.history) != 2 || len(e.observers) != 4 || len(e.failures) != 4 {
		return false
	}
	for i, id := range []string{projectionID, "$system.statistics.event-types", "$system.statistics.event-types.global", "history-observer"} {
		info := e.observers[i]
		if info == nil || info.Id != id || info.EventSequenceId != "event-log" ||
			info.RunningState != contracts.ObserverRunningState_Active || !info.IsSubscribed ||
			info.TailEventSequenceNumber != 1 || info.HandledEventCount != 1 ||
			e.failures[i] == nil || len(e.failures[i].Items) != 0 {
			return false
		}
		if i < 3 {
			if info.Type != contracts.ObserverType_Projection || info.Owner != contracts.ObserverOwner_Kernel ||
				info.LastHandledEventSequenceNumber != 0 || info.NextEventSequenceNumber != 1 {
				return false
			}
		} else if info.Type != contracts.ObserverType_Reactor || info.Owner != contracts.ObserverOwner_Client ||
			info.LastHandledEventSequenceNumber != 1 || info.NextEventSequenceNumber != 2 {
			return false
		}
	}
	for i, id := range []events.TypeID{"ProjectionAccountOpened", "ProjectionAccountRenamed"} {
		event := e.history[i]
		if event.Context.SourceID != source || event.Context.SequenceNumber != events.SequenceNumber(i) ||
			event.Context.EventType != (events.TypeRef{ID: id, Generation: 1}) || len(event.Revisions) != 0 {
			return false
		}
	}
	var opened, renamed map[string]string
	return e.last.Value.Occurred.Equal(e.history[0].Context.Occurred) &&
		json.Unmarshal(e.history[0].Content, &opened) == nil && maps.Equal(opened, map[string]string{"fullName": "original", "productName": "", "local": "", "excluded": ""}) &&
		json.Unmarshal(e.history[1].Content, &renamed) == nil && maps.Equal(renamed, map[string]string{"name": "before"})
}
