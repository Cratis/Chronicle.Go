// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
)

type OrderPlaced struct{}

// operations-wait:start
func appendAndWaitForOperations(ctx context.Context, store *chronicle.EventStore) error {
	appended, err := store.EventLog().AppendWithMetadata(ctx, "order-42", OrderPlaced{})
	if err != nil {
		return err
	}
	if err := appended.Result().Err(); err != nil {
		return err
	}
	processed, err := appended.WaitForCompletion(ctx, store.Observers(), 0)
	if err != nil {
		return err
	}
	if !processed.IsSuccess() {
		return fmt.Errorf("processing incomplete: timed out=%t, outstanding=%v",
			processed.TimedOut(), processed.OutstandingObservers())
	}
	return nil
}

// operations-wait:end

func TestOperationsDocumentationMatchesCompiledExample(t *testing.T) {
	checkProjectionSnippets(t, "operations_documentation_test.go", "Documentation/operations.md", []string{"operations-wait"})
	// Keep the compiling workflow reachable without claiming it runs against a server here.
	_ = appendAndWaitForOperations
}
