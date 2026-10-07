// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/kernelcapability"
)

// KeyProperty returns the serialized root ID property, or empty when undeclared.
func (d Descriptor) KeyProperty() string { return idProperty(d) }

// ReplayProjection computes all instances by replaying at most eventCount events
// through the kernel projection. This does not wait for or prove sink/observer
// catch-up. Counts above math.MaxInt32 are invalid; zero returns an empty non-nil
// result without an RPC.
// Reducers are explicitly unsupported here. The kernel releases classified values
// with the subject each was written under (19.32.2 or later, Chronicle#4561); the
// SDK never decrypts them again, and refuses classified models with ErrUnsupported
// unless the connection reports such a kernel. Returned JSON is owned and ID aliases
// are normalized by the same path as Get. No local projection engine is used.
func (s *Service) ReplayProjection(ctx context.Context, model Identifier, eventCount uint64) ([]json.RawMessage, error) {
	// Needs added by admission travel with every RPC of this read and are
	// re-checked against the generation that dispatches it.
	ctx = kernelcapability.Track(ctx)
	d, ok := s.catalog.LookupIdentifier(model)
	if !ok {
		return nil, notRegistered()
	}
	kind, id := d.Observer()
	if kind != Projection || id == "" {
		return nil, fmt.Errorf("%w: registered projection required", faults.ErrUnsupported)
	}
	if eventCount > math.MaxInt32 {
		return nil, invalid("replay count exceeds kernel limit")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if eventCount == 0 {
		return []json.RawMessage{}, nil
	}
	if err := s.projectionReleaseAdmission(ctx, d); err != nil {
		return nil, err
	}
	// Existing adapters may own producer admission themselves. Reject a known
	// unsupported shape, but preserve legacy admission for unknown producers.
	// New GetAll/GetSnapshots require affirmative fidelity evidence instead.
	if s.replayValidator != nil {
		if _, err := s.replayValidator(ctx, d); err != nil {
			return nil, err
		}
	}
	collection, err := s.readCollection(ctx, d, events.Count(eventCount))
	if err != nil {
		return nil, readFailure(err)
	}
	result := make([]json.RawMessage, len(collection.Instances))
	for i, instance := range collection.Instances {
		result[i] = instance.Value
	}
	return result, nil
}
