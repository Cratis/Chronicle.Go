// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"slices"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

type metadataEvent struct{}

func TestPlanRegistrationMetadataAndDetachedTags(t *testing.T) {
	event, err := events.Define[metadataEvent]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	// Part 2 authoring options will populate these fields. Keep their transport
	// contract independent of OnceOnly handler/replay behavior in this slice.
	declaration, err := DefineHandler("metadata", func(context.Context, metadataEvent) error { return nil }, func(c *configuration) {
		c.replayable = false
		c.tags = []string{"artifact"}
		c.filterTags = []string{"event"}
		c.sourceType = "customer"
		c.streamType = "orders"
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Compile(declaration, catalog, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	if plan.IsReplayable() || plan.EventSourceType() != "customer" || plan.EventStreamType() != "orders" {
		t.Fatal("registration metadata was not preserved")
	}
	plan.Tags()[0] = "mutated"
	plan.FilterTags()[0] = "mutated"
	if !slices.Equal(plan.Tags(), []string{"artifact"}) || !slices.Equal(plan.FilterTags(), []string{"event"}) {
		t.Fatal("plan exposes mutable tags")
	}
}
