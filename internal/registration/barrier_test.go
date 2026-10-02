// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package registration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestSingleFlightRetriesAndSnapshots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var barrier Barrier
		entered, release := make(chan struct{}), make(chan struct{})
		attempts := 0
		transient := errors.New("transient")
		run := func(context.Context) ([]Artifact, error) {
			attempts++
			if attempts == 1 {
				close(entered)
				<-release
				return []Artifact{{Name: "store"}, {Name: "types", Failure: transient}}, transient
			}
			return []Artifact{{Name: "store"}, {Name: "types"}}, nil
		}
		policy := Policy{2, time.Second, 2 * time.Second, time.Minute}
		var group sync.WaitGroup
		results := make(chan Outcome, 8)
		for range 8 {
			group.Go(func() {
				results <- barrier.Run(t.Context(), 7, policy, func(err error) bool { return errors.Is(err, transient) }, run)
			})
		}
		<-entered
		synctest.Wait()
		close(release)
		group.Wait()
		close(results)
		count := 0
		for outcome := range results {
			count++
			if !outcome.IsSuccess() || outcome.Attempts != 2 || outcome.Generation != 7 || outcome.Pass != 1 {
				t.Fatalf("%+v", outcome)
			}
			outcome.Artifacts[0].Name = "mutated"
		}
		if count != 8 || attempts != 2 || barrier.Snapshot().Artifacts[0].Name != "store" {
			t.Fatal("single-flight or snapshot broken")
		}
	})
}

func TestCanceledWaiterDoesNotCancelOwnerAndFailureCanRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var barrier Barrier
		release := make(chan struct{})
		first := make(chan Outcome, 1)
		expected := errors.New("rejected")
		policy := Policy{1, time.Second, time.Second, time.Minute}
		go func() {
			first <- barrier.Run(t.Context(), 1, policy, func(error) bool { return false }, func(context.Context) ([]Artifact, error) {
				<-release
				return []Artifact{{Name: "store"}, {Name: "types", Failure: expected}}, expected
			})
		}()
		synctest.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		outcome := barrier.Run(ctx, 1, policy, func(error) bool { return false }, nil)
		if !errors.Is(outcome.Failure, context.Canceled) {
			t.Fatal(outcome)
		}
		close(release)
		outcome = <-first
		if outcome.IsSuccess() || !errors.Is(outcome.Failure, expected) || len(outcome.Artifacts) != 2 {
			t.Fatal(outcome)
		}
		outcome = barrier.Run(t.Context(), 1, policy, func(error) bool { return false }, func(context.Context) ([]Artifact, error) { return nil, nil })
		if !outcome.IsSuccess() || outcome.Pass != 2 {
			t.Fatal("failure poisoned barrier", outcome)
		}
	})
}
