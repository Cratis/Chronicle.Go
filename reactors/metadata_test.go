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
	labels := []string{"artifact", "second", "artifact", "second"}
	filters := []string{"event", "other", "event", "other"}
	options := []Option{WithTags("replaced"), WithEventTagFilter("replaced"), WithTags(labels...), WithEventTagFilter(filters...)}
	labels[0] = "mutated"
	filters[0] = "mutated"
	options = append(options, func(c *configuration) {
		c.replayable = false
		c.sourceType = "customer"
		c.streamType = "orders"
	})
	declaration, err := DefineHandler("metadata", func(context.Context, metadataEvent) error { return nil }, options...)
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
	if !slices.Equal(plan.Tags(), []string{"artifact", "second"}) || !slices.Equal(plan.FilterTags(), []string{"event", "other"}) {
		t.Fatal("plan tags were not deduplicated or detached", plan.Tags(), plan.FilterTags())
	}
}
