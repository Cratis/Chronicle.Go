// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"encoding/json"
	"fmt"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
)

// KeyProperty returns the serialized root ID property, or empty when undeclared.
func (d Descriptor) KeyProperty() string { return idProperty(d) }

// ReplayProjection computes all instances by replaying at most eventCount events
// through the kernel projection. This does not wait for or prove sink/observer
// catch-up. The count must be finite (not MaxUint64); zero means no events.
// Reducers are explicitly unsupported here. Returned JSON is owned and ID aliases
// are normalized by the same path as Get. No local projection engine is used.
func (s *Service) ReplayProjection(ctx context.Context, model Identifier, eventCount uint64) ([]json.RawMessage, error) {
	d, ok := s.catalog.LookupIdentifier(model)
	if !ok {
		return nil, notRegistered()
	}
	kind, id := d.Observer()
	if kind != Projection || id == "" {
		return nil, fmt.Errorf("%w: registered projection required", faults.ErrUnsupported)
	}
	if eventCount == ^uint64(0) {
		return nil, invalid("finite replay count required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := s.client.GetAllInstances(ctx, &contracts.GetAllInstancesRequest{EventStore: string(s.store), Namespace: string(s.namespace), ReadModelIdentifier: string(model), EventSequenceId: string(d.EventSequence()), EventCount: eventCount})
	if err != nil {
		return nil, wire.RPCError(err)
	}
	if response == nil {
		return nil, faults.ErrProtocol
	}
	result := make([]json.RawMessage, len(response.Instances))
	for i, instance := range response.Instances {
		if !validDocument([]byte(instance)) {
			return nil, faults.ErrProtocol
		}
		result[i], err = normalizeID([]byte(instance), d)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
