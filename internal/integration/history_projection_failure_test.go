//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
)

// This identity is produced only by exhaustion of the local polling loop after
// successful reads, never by a Reader.Get error (including remote deadlines).
var errInitialHistoryProjectionPollExhausted = errors.New("initial history projection polling budget exhausted")

func awaitInitialHistoryProjection(t *testing.T, f *kernelFixture, store *chronicle.EventStore, reader *readmodels.Reader[ProjectionAccount], source events.SourceID, projectionID string, observed <-chan historyObservation) {
	t.Helper()
	last, err := pollInitialHistoryProjection(f.ctx, reader, source)
	if err == nil {
		return
	}
	// Diagnose before client cleanup, but never turn an unexplained projection
	// failure into a skip. Public snapshots cannot establish the shared-set
	// cause in https://github.com/Cratis/Chronicle/issues/4558; disappeared jobs
	// do not prove successful catch-up and one matching symptom is insufficient.
	if err != errInitialHistoryProjectionPollExhausted || f.ctx.Err() != nil {
		t.Fatalf("initial history projection: %+v: %v", last, err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	for _, id := range []string{projectionID, "$system.statistics.event-types", "$system.statistics.event-types.global", "history-observer"} {
		info, infoErr := contracts.NewObserversClient(f.conn).GetObserverInformation(ctx, &contracts.GetObserverInformationRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), EventSequenceId: "event-log", ObserverId: id})
		failures, failuresErr := contracts.NewFailedPartitionsClient(f.conn).GetFailedPartitions(ctx, &contracts.GetFailedPartitionsRequest{EventStore: string(f.storeName), Namespace: string(store.Namespace()), ObserverId: id})
		t.Logf("initial history projection store=%s observer=%v (%v), failures=%v (%v)", f.storeName, info, infoErr, failures, failuresErr)
		if infoErr != nil || failuresErr != nil {
			t.Fatal("could not diagnose initial history projection timeout", err)
		}
	}
	history, historyErr := store.EventLog().ReadSource(ctx, source, eventsequences.SourceFilter{})
	if historyErr != nil {
		t.Fatal("read history after initial projection timeout", historyErr)
	}
	var observations []historyObservation
	for len(observed) > 0 {
		observations = append(observations, <-observed)
	}
	t.Fatalf("initial history projection store=%s last=%+v observations=%+v history=%+v: %v", f.storeName, last, observations, history, err)
}

func pollInitialHistoryProjection(parent context.Context, reader *readmodels.Reader[ProjectionAccount], source events.SourceID) (readmodels.Instance[ProjectionAccount], error) {
	return pollHistoryProjection(parent, 15*time.Second, func(ctx context.Context) (readmodels.Instance[ProjectionAccount], error) {
		return reader.Get(ctx, readmodels.Key(source))
	})
}

func pollHistoryProjection(parent context.Context, budget time.Duration, read func(context.Context) (readmodels.Instance[ProjectionAccount], error)) (readmodels.Instance[ProjectionAccount], error) {
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var last readmodels.Instance[ProjectionAccount]
	for {
		value, err := read(ctx)
		if err != nil {
			// Preserve the read's failure even if local cancellation/deadline
			// occurs concurrently. A wire RPC deadline also matches
			// context.DeadlineExceeded, but is not polling exhaustion.
			return last, err
		}
		last = value
		if value.Exists && value.Value.Name == "before" {
			return value, nil
		}
		select {
		case <-ctx.Done():
			if err := parent.Err(); err != nil {
				return last, err
			}
			return last, errInitialHistoryProjectionPollExhausted
		case <-ticker.C:
		}
	}
}
