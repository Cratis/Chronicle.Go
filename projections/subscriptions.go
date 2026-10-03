// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"github.com/cratis/chronicle.go/events"
	"slices"
	"strings"
)

// ExternalSubscription returns the source implied by an inbox sequence and its
// distinct event IDs. Like C#, an explicitly selected inbox also provisions;
// an explicit non-inbox sequence does not. Go includes child/nested/removal
// handlers as well as root from/join handlers, matching its broader inference.
func (d Definition) ExternalSubscription() (string, []events.TypeID) {
	sequence := string(d.EventSequence())
	if d.data == nil || !strings.HasPrefix(sequence, events.InboxPrefix) {
		return "", nil
	}
	source := strings.TrimPrefix(sequence, events.InboxPrefix)
	ids := map[events.TypeID]bool{}
	var visit func(*nodeDefinition)
	visit = func(n *nodeDefinition) {
		for _, from := range n.from {
			ids[from.event.ID] = true
		}
		for _, join := range n.joins {
			ids[join.event.ID] = true
		}
		for _, removal := range n.removals {
			ids[removal.event.ID] = true
		}
		for _, child := range n.children {
			visit(child)
		}
		for _, nested := range n.nested {
			visit(nested)
		}
	}
	visit(&d.data.nodeDefinition)
	result := make([]events.TypeID, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	slices.Sort(result)
	return source, result
}
