// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package identities describes actors responsible for events, not authentication credentials.
package identities

// Identity describes an actor and an ordered on-behalf-of chain. Snapshot before
// sharing with concurrent callers; fields remain caller-owned until captured.
type Identity struct {
	// Subject is the stable actor identifier.
	Subject string
	// Name is the actor's display name.
	Name string
	// UserName is the optional login name.
	UserName string
	// OnBehalfOf is the actor represented by this actor, or nil.
	OnBehalfOf *Identity
}

// NotSet returns Chronicle's canonical absent-actor identity.
func NotSet() Identity {
	return Identity{Subject: "1efc9b81-0612-4466-962c-86acc4e9a028", Name: "[Not Set]", UserName: "[Not Set]"}
}

// Snapshot copies the chain, keeping the first occurrence of each subject in
// order. Cyclic pointer chains are cut at the first repeated node.
func (i Identity) Snapshot() Identity {
	seen := make(map[string]bool)
	pointers := make(map[*Identity]bool)
	var chain []Identity
	for current := &i; current != nil && !pointers[current]; current = current.OnBehalfOf {
		pointers[current] = true
		if !seen[current.Subject] {
			seen[current.Subject] = true
			chain = append(chain, Identity{Subject: current.Subject, Name: current.Name, UserName: current.UserName})
		}
	}
	var result *Identity
	for n := len(chain) - 1; n >= 0; n-- {
		next := chain[n]
		next.OnBehalfOf = result
		result = &next
	}
	return *result
}
