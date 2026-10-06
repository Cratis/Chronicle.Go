// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
)

// keyResolverEventTypeIDs mirrors Chronicle v19.29.4's resolver-map membership,
// not replay admission or projection evaluation. Keys do not change the set.
func keyResolverEventTypeIDs(wire *contracts.ProjectionDefinition) ([]events.TypeID, bool) {
	root := &contracts.ChildrenDefinition{
		From: wire.From, Join: wire.Join, RemovedWith: wire.RemovedWith,
		RemovedWithJoin: wire.RemovedWithJoin, Children: wire.Children,
		Nested: wire.Nested, FromEventProperty: wire.FromEventProperty,
	}
	ids := childIDs(root, false)
	// ProjectionFactory.cs:1040-1046; wire FromEvery is kernel FromDerivatives
	// (Grpc/Projections/Definitions/ProjectionDefinitionConverters.cs:36-37).
	for _, derivative := range wire.FromEvery {
		for _, event := range derivative.GetEventTypes() {
			ids = append(ids, events.TypeID(event.GetId()))
		}
	}
	// Projection.cs:168-169,218-238: membership is by ID, ignoring generation.
	seen := make(map[events.TypeID]struct{}, len(ids))
	unique := make([]events.TypeID, 0, len(ids))
	for _, id := range ids {
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			unique = append(unique, id)
		}
	}
	return unique, wire.SubscribesToAllEvents
}

func childIDs(node *contracts.ChildrenDefinition, hasParent bool) []events.TypeID {
	// ProjectionFactory.cs:1011-1016: joins count at every collection depth,
	// but join removals count only on children, never on the root.
	ids := pairIDs(node.GetFrom())
	ids = append(ids, pairIDs(node.GetJoin())...)
	ids = append(ids, pairIDs(node.GetRemovedWith())...)
	if hasParent {
		ids = append(ids, pairIDs(node.GetRemovedWithJoin())...)
	}
	ids = append(ids, nestedIDs(node.GetNested(), hasParent)...)
	// ProjectionFactory.cs:1048-1051: event-property subscriptions count on
	// roots and children, but not inside CollectNestedEventTypes.
	if property := node.GetFromEventProperty(); property != nil {
		ids = append(ids, events.TypeID(property.GetEvent().GetId()))
	}
	// ProjectionFactory.cs:625-645,1055-1059: include each child's full subtree.
	for _, child := range node.GetChildren() {
		ids = append(ids, childIDs(child, true)...)
	}
	return ids
}

func nestedIDs(nested map[string]*contracts.ChildrenDefinition, hasParent bool) []events.TypeID {
	var ids []events.TypeID
	// ProjectionFactory.cs:1094-1131: only From, RemovedWith, root-level Join,
	// and recursive Nested contribute. Children, join removals, event-property
	// subscriptions and All mappings within a nested object do not contribute.
	for _, node := range nested {
		ids = append(ids, pairIDs(node.GetFrom())...)
		if !hasParent {
			ids = append(ids, pairIDs(node.GetJoin())...)
		}
		ids = append(ids, pairIDs(node.GetRemovedWith())...)
		ids = append(ids, nestedIDs(node.GetNested(), hasParent)...)
	}
	return ids
}

func pairIDs[T interface{ GetKey() *contracts.EventType }](pairs []T) []events.TypeID {
	ids := make([]events.TypeID, 0, len(pairs))
	for _, pair := range pairs {
		ids = append(ids, events.TypeID(pair.GetKey().GetId()))
	}
	return ids
}
