// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/fundamentals.go/concepts"
	"github.com/cratis/fundamentals.go/correlation"
)

func TestFundamentalsContextReachesAppendCorrelationID(t *testing.T) {
	id, err := concepts.ParseUUID("00112233-4455-6677-8899-aabbccddeeff")
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan *sequences.AppendRequest, 1)
	client, _ := testClient(t, &fakeKernel{append: func(_ context.Context, r *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		requests <- r
		return success(r, 0), nil
	}})
	ctx := correlation.WithID(testContext(t), id)
	store, err := client.EventStore(ctx, "shared-correlation")
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.EventLog().Append(ctx, "source", CustomerRegistered{Name: "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if wire.Correlation(request.CorrelationId) != metadata.CorrelationID([16]byte(id)) || [16]byte(result.CorrelationID) != [16]byte(id) {
		t.Fatalf("correlation bytes changed: %v", request.CorrelationId)
	}
}
