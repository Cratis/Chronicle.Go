// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/events"
)

func TestExternalSubscriptionIncludesAllHandlersAndExplicitInbox(t *testing.T) {
	event := func(id events.TypeID) fromDefinition {
		return fromDefinition{event: events.TypeRef{ID: id, Generation: 3}}
	}
	d := Definition{data: &definition{sequence: "inbox-origin", sequenceExplicit: true, nodeDefinition: nodeDefinition{
		from: []fromDefinition{event("a")}, joins: []joinDefinition{{fromDefinition: event("b")}}, removals: []removalDefinition{{event: events.TypeRef{ID: "c", Generation: 1}}},
		children: map[string]*nodeDefinition{"children": {from: []fromDefinition{event("d")}}}, nested: map[string]*nodeDefinition{"nested": {from: []fromDefinition{event("a"), event("e")}}},
	}}}
	source, ids := d.ExternalSubscription()
	if source != "origin" || !reflect.DeepEqual(ids, []events.TypeID{"a", "b", "c", "d", "e"}) {
		t.Fatal(source, ids)
	}
	ids[0] = "changed"
	_, fresh := d.ExternalSubscription()
	if fresh[0] != "a" {
		t.Fatal("IDs were not detached")
	}
	d.data.sequence = events.EventLog
	if source, ids = d.ExternalSubscription(); source != "" || len(ids) != 0 {
		t.Fatal("explicit local sequence provisioned")
	}
}
