//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation"
)

// eventLogStatisticsObservers are the kernel-owned event-log observers every
// store has. They observe all event types, so they share the test's partitions.
var eventLogStatisticsObservers = []string{"$system.statistics.event-types", "$system.statistics.event-types.global"}

// awaitObserversObserving waits until each named event-log observer has a live
// subscription in the kernel's observing state (running state Active).
//
// WaitForRegistration only proves that client reactor and reducer streams are
// open; the kernel subscribes those observers asynchronously afterwards. Kernel
// 19.32.3 fixed dropped live delivery after a first-subscription catch-up
// (https://github.com/Cratis/Chronicle/issues/4558), so most tests append right
// after registration. The explicit-reconnect cache test still needs this wait:
// without it the reactor received the previous event again after reconnect.
// It polls server state; it does not retry or relax any assertion.
func awaitObserversObserving(t *testing.T, f *kernelFixture, namespace chronicle.Namespace, ids ...string) {
	t.Helper()
	client := contracts.NewObserversClient(f.conn)
	pending := append([]string(nil), ids...)
	last := make(map[string]*contracts.ObserverInformation, len(ids))
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		remaining := pending[:0]
		for _, id := range pending {
			info, err := client.GetObserverInformation(f.ctx, &contracts.GetObserverInformationRequest{EventStore: string(f.storeName), Namespace: string(namespace), EventSequenceId: "event-log", ObserverId: id})
			if err != nil {
				t.Fatalf("observer %s readiness: %v", id, err)
			}
			last[id] = info
			if !info.GetIsSubscribed() || info.GetRunningState() != contracts.ObserverRunningState_Active || info.GetEventSequenceId() != "event-log" {
				remaining = append(remaining, id)
			}
		}
		if pending = remaining; len(pending) == 0 {
			return
		}
		select {
		case <-f.ctx.Done():
			for _, id := range pending {
				t.Logf("observer %s not observing: %v", id, last[id])
			}
			t.Fatalf("observers did not reach the observing state before appends: %v", f.ctx.Err())
		case <-ticker.C:
		}
	}
}
