// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package decision

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
)

func TestCleanupContextDetachesOnlyReadValidationAndCancellation(t *testing.T) {
	type metadataKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), metadataKey{}, "retained"))
	ctx = WithDispatchValidation(ctx, func() error { return ErrStale })
	cancel()
	cleanup := CleanupContext(ctx)
	if cleanup.Err() != nil || cleanup.Done() != nil || cleanup.Value(metadataKey{}) != "retained" || ValidateDispatch(cleanup) != nil {
		t.Fatal("cleanup retained read validation/cancellation or lost metadata")
	}
	if !errors.Is(ValidateDispatch(ctx), ErrStale) || !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("cleanup changed the original read context")
	}
}

func TestFrozenCatalogAndIssuedTokenOwnTheirInputs(t *testing.T) {
	projection := &projections.ProjectionDefinition{Identifier: "projection", All: &projections.FromEveryDefinition{Properties: map[string]string{"value": "source"}}}
	catalog := NewCatalog([]*projections.ProjectionDefinition{projection}, nil)
	projection.All.Properties["value"] = "mutated"
	if catalog.Projections[0].All.Properties["value"] != "source" {
		t.Fatal("projection catalog borrowed mutable AST")
	}
	target := Target{Client: t, Store: "store", Namespace: "namespace", Sequence: string(events.EventLog)}
	types := []events.TypeRef{{ID: "changed", Generation: 1}}
	token := Issue(Evidence{Target: target, Model: "model", Key: "source", Types: types, Boundary: 5, Catalog: catalog, Epoch: catalog.Epoch.Load(), Generation: 1, Check: func() error { return nil }})
	types[0].ID = "mutated"
	var guard *Guard
	if err := Enroll(token, target, t, func(g *Guard) error { guard = g; return nil }); err != nil {
		t.Fatal(err)
	}
	evidence := guard.Evidence()
	evidence.Types[0].ID = "mutated again"
	if guard.Evidence().Types[0].ID != "changed" {
		t.Fatal("token evidence was mutable")
	}
	catalog.Epoch.Add(1)
	if err := guard.Validate(target, 1); !errors.Is(err, ErrStale) {
		t.Fatalf("old catalog remained valid: %v", err)
	}
}
