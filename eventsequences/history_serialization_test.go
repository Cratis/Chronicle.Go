// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"math"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/serialization"
)

type revisedGeneration struct {
	FullName string
	Amount   float64
}

func TestRevisionUsesSelectedGenerationSerializationPlan(t *testing.T) {
	definition, err := events.Define[revisedGeneration](events.WithID("evolved"), events.WithGeneration(3))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := definition.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	sequence, calls := parityFixture(t, map[string]rpcHandler{"Revise": func(_ context.Context, value any) (any, error) {
		r := value.(*sequences.ReviseRequest)
		if r.EventType.Id != "evolved" || r.EventType.Generation != 3 || r.Content != `{"fullName":"new name","amount":42}` {
			t.Error(r)
		}
		return &sequences.CommandResult{IsAuthorized: true}, nil
	}}, catalog, eventsequences.ConcurrencyPolicy{})
	ctx := testContext(t)
	if err := sequence.Revise(ctx, 0, &revisedGeneration{"new name", 42}); err != nil {
		t.Fatal(err)
	}
	if err := sequence.Revise(ctx, 0, revisedGeneration{Amount: math.NaN()}); err == nil {
		t.Fatal("unserializable replacement accepted")
	}
	if err := sequence.Revise(ctx, 0, (*revisedGeneration)(nil)); err == nil {
		t.Fatal("nil replacement accepted")
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}
