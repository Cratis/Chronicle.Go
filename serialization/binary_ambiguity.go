// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import "strings"

// A literal dotted name and a segmented path can denote different properties in
// the kernel. Reject binary-involved collisions in the plan, not one consumer at
// a time. Ordinary collisions retain their historical first-match behavior.
func validateBinaryPathAmbiguity(root *node) error {
	if !root.containsBinary {
		return nil
	}
	return visitNodes(root, map[*node]bool{}, func(n *node) error {
		for _, f := range n.fields {
			if !strings.Contains(f.name, ".") {
				continue
			}
			count, binary := binaryPathCandidates(n, f.name)
			if count > 1 && binary {
				return unsupported(n.typ, "binary-involved dotted property path ambiguity is not supported")
			}
		}
		return nil
	})
}

// Resolve every partition of a finite serialized path by emitted field names.
// Unlike flattened metadata this reaches beyond recursive edges, and unlike
// strings.Split it accounts for literal dots at intermediate object levels too.
// Each field step consumes a nonempty name, so recursive types terminate.
func binaryPathCandidates(n *node, path string) (int, bool) {
	for n.reference != nil || n.item != nil {
		if n.reference != nil {
			n = n.reference
		} else {
			n = n.item
		}
	}
	count, binary := 0, false
	for _, f := range n.fields {
		if f.name == path {
			count++
			binary = binary || f.value.containsBinary
		} else if remainder, ok := strings.CutPrefix(path, f.name+"."); ok {
			candidates, contains := binaryPathCandidates(f.value, remainder)
			count += candidates
			binary = binary || contains
		}
	}
	return count, binary
}
