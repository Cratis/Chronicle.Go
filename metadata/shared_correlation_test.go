// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package metadata_test

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/fundamentals.go/concepts"
	"github.com/cratis/fundamentals.go/correlation"
)

func TestFundamentalsCorrelationContextRoundTrip(t *testing.T) {
	id, err := concepts.ParseUUID("00112233-4455-6677-8899-aabbccddeeff")
	if err != nil {
		t.Fatal(err)
	}
	ctx := correlation.WithID(t.Context(), id)
	if got := metadata.Correlation(ctx); [16]byte(got) != [16]byte(id) {
		t.Fatal(got)
	}
	back := metadata.WithCorrelation(t.Context(), metadata.CorrelationID([16]byte(id)))
	if correlation.FromContext(back) != id {
		t.Fatal("Chronicle context not shared")
	}
	zero := metadata.WithCorrelation(ctx, metadata.CorrelationID{})
	if correlation.FromContext(zero) != (concepts.UUID{}) || metadata.Correlation(zero) != (metadata.CorrelationID{}) {
		t.Fatal("zero did not shadow parent")
	}
	if metadata.Correlation(t.Context()) != (metadata.CorrelationID{}) {
		t.Fatal("read generated a correlation")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if metadata.WithCorrelation(cancelled, metadata.CorrelationID{}).Err() != context.Canceled {
		t.Fatal("cancellation lost")
	}
	// Keep the existing wider parser, independently of shared strict UUID parsing.
	parsed, err := metadata.ParseCorrelationID("00112233445566778899aabbccddeeff")
	if err != nil || [16]byte(parsed) != [16]byte(id) {
		t.Fatalf("parser compatibility: %v", err)
	}
}
