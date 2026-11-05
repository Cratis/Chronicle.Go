// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reactors"
)

func TestReactorScenarioDeliversGivenEventsToABatchMethodAsOnePage(t *testing.T) {
	registry := eventRegistry(t)
	var batches []int
	if err := chronicle.RegisterReactorHandlers(registry, "indexer", []reactors.Handler{
		reactors.OnBatch(func(_ context.Context, batch *observation.EventBatch, bc observation.BatchContext) error {
			batches = append(batches, batch.Len())
			if bc.Origin != observation.OriginLive || !bc.CanDefer {
				return errors.New("scenario pages are live pages from a kernel that delivers pages")
			}
			for _, item := range batch.All() {
				if item.Event.(AccountOpened).Name == "later" {
					return batch.Defer(item.Context.SourceID, time.Minute)
				}
			}
			return nil
		}, observation.Observes[AccountOpened]()),
	}); err != nil {
		t.Fatal(err)
	}
	s := chronicletest.NewReactorScenarioForID(t, chronicletest.Config{Registry: registry}, "indexer")
	if err := s.Given(t.Context(), "one", AccountOpened{Name: "Ada"}, AccountOpened{Name: "Grace"}); err != nil {
		t.Fatal(err)
	}
	err := s.Given(t.Context(), "two", AccountOpened{Name: "later"})
	var deferred *observation.DeferredError
	if !errors.As(err, &deferred) || deferred.RetryAfter != time.Minute || deferred.Partition != "two" {
		t.Fatalf("deferral = %v", err)
	}
	if len(batches) != 2 || batches[0] != 2 {
		t.Fatalf("batches = %v", batches)
	}
}
