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

func TestSharedReadinessSurvivesExplicitMutationCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var barrier Barrier
		var work sync.WaitGroup
		defer work.Wait()
		policy := Policy{1, time.Second, time.Second, time.Minute}
		mutationCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		entered, owned, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		mutation, readiness := make(chan Outcome, 1), make(chan Outcome, 1)
		go func() {
			mutation <- barrier.Run(mutationCtx, 7, policy, func(error) bool { return true }, func(ctx context.Context) ([]Artifact, error) {
				close(entered)
				<-ctx.Done()
				return []Artifact{{Name: "projections", Failure: ctx.Err()}}, ctx.Err()
			})
		}()
		<-entered
		starts := 0
		go func() {
			readiness <- barrier.RunShared(t.Context(), 7, policy, func(error) bool { return true }, func(ctx context.Context) ([]Artifact, error) {
				close(owned)
				select {
				case <-release:
					return []Artifact{{Name: "projections"}}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}, func(run func(context.Context)) error {
				starts++
				work.Go(func() { run(t.Context()) })
				return nil
			})
		}()
		synctest.Wait()
		cancel()
		outcome := <-mutation
		if !errors.Is(outcome.Failure, context.Canceled) || len(outcome.Artifacts) != 1 {
			t.Fatal("explicit mutation lost its stage outcome", outcome)
		}
		<-owned
		close(release)
		outcome = <-readiness
		if !outcome.IsSuccess() || outcome.Pass != 2 || outcome.Attempts != 1 || starts != 1 {
			t.Fatal("readiness inherited explicit caller cancellation", outcome, starts)
		}
	})
}
