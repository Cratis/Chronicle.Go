// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/transactions"
)

func TestStageRacingCommitNeverLosesSuccessfulEnrollment(t *testing.T) {
	var persisted atomic.Int32
	ctx, sequence, calls := fixture(t, func(_ context.Context, input any) (any, error) {
		request := input.(*sequences.AppendManyForEventSourcesRequest)
		persisted.Store(int32(len(request.Events)))
		return success(len(request.Events)), nil
	})
	beginCtx, cancel := context.WithCancel(ctx)
	unit, owner := begin(t, beginCtx, sequence)
	cancel() // Begin doesn't retain a request context or cancel independent work.
	stage(t, ctx, unit, "A", "seed")
	start := make(chan struct{})
	var wg sync.WaitGroup
	var staged atomic.Int32
	staged.Store(1)
	for i := range 32 {
		wg.Go(func() {
			<-start
			err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: changed{Value: strconv.Itoa(i)}}})
			if err == nil {
				staged.Add(1)
			} else if !errors.Is(err, transactions.ErrCompleting) && !errors.Is(err, transactions.ErrCompleted) {
				t.Error(err)
			}
		})
	}
	wg.Go(func() {
		<-start
		result, err := owner.Commit(ctx)
		if err != nil || result.Err() != nil {
			t.Error(result, err)
		}
	})
	close(start)
	wg.Wait()
	if persisted.Load() != staged.Load() || calls.Load() != 1 || unit.State() != transactions.Committed {
		t.Fatalf("persisted=%d staged=%d calls=%d state=%v", persisted.Load(), staged.Load(), calls.Load(), unit.State())
	}
}
