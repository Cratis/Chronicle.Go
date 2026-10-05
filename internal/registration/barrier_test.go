// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package registration

import (
	"context"
	"errors"
	"fmt"
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

func TestLiveJoinerRetriesCanceledStarter(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var barrier Barrier
				starterCtx, cancel := context.WithCancel(t.Context())
				if deadline {
					cancel()
					starterCtx, cancel = context.WithTimeout(t.Context(), time.Second)
				}
				defer cancel()
				policy := Policy{1, time.Second, time.Second, time.Minute}
				entered := make(chan struct{})
				starter, joiner := make(chan Outcome, 1), make(chan Outcome, 1)
				calls := 0
				run := func(ctx context.Context) ([]Artifact, error) {
					calls++
					if calls == 1 {
						close(entered)
						<-ctx.Done()
						return nil, fmt.Errorf("registration: %w", ctx.Err())
					}
					return []Artifact{{Name: "store"}}, nil
				}
				go func() { starter <- barrier.Run(starterCtx, 1, policy, func(error) bool { return true }, run) }()
				<-entered
				go func() { joiner <- barrier.Run(t.Context(), 1, policy, func(error) bool { return true }, run) }()
				synctest.Wait()
				if deadline {
					time.Sleep(time.Second) // Advance fake time to the starter's deadline.
				} else {
					cancel()
				}
				if outcome := <-starter; !errors.Is(outcome.Failure, starterCtx.Err()) {
					t.Fatal(outcome)
				}
				if outcome := <-joiner; !outcome.IsSuccess() || outcome.Pass != 2 || calls != 2 {
					t.Fatalf("live joiner inherited starter failure: %+v, calls=%d", outcome, calls)
				}
			})
		})
	}
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
