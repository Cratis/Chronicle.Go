// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

type customerRegistered struct {
	Name string `json:"name"`
}

// Import reads the entries lazily, so a source of any size is never held in
// memory at once. This example compiles; running it needs a kernel.
func ExampleSequence_Import() {
	run := func(ctx context.Context, eventLog *eventsequences.Sequence) error {
		names := []string{"Ada", "Grace"}
		source := func(yield func(eventsequences.Entry, error) bool) {
			for i, name := range names {
				entry := eventsequences.Entry{Source: events.SourceID(fmt.Sprint("customer-", i)), Event: customerRegistered{Name: name},
					IdempotencyKey: eventsequences.IdempotencyKey(fmt.Sprint("legacy-", i))}
				if !yield(entry, nil) {
					return
				}
			}
		}
		result, err := eventLog.Import(ctx, source)
		if err != nil {
			return err
		}
		if !result.IsSuccess() {
			return fmt.Errorf("import %s %s: %d rejected", result.ImportID, result.State, result.RejectedCount)
		}
		return nil
	}
	_ = run
}
