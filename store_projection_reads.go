// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type projectionReadPolicy struct {
	definition *contracts.ProjectionDefinition
	model      readmodels.Descriptor
	protected  bool
}

// Capture the selected, store-bound snapshot, not a callback into mutable client
// registries. KernelDefinition returns a deep copy; schemas/catalogs are frozen.
// Reinitializing inbox artifacts installs a new service with a new policy.
func projectionReplayValidatorFor(snapshot registrySnapshot) (readmodels.ProjectionReplayValidator, error) {
	policies := make(map[readmodels.Identifier]projectionReadPolicy, len(snapshot.projections))
	for _, projection := range snapshot.projections {
		model, ok := snapshot.models.LookupIdentifier(projection.Model().Identifier())
		if !ok {
			return nil, ErrNotRegistered
		}
		roots, err := serialization.ProtectionRoots(model.Schema())
		if err != nil {
			return nil, err
		}
		policies[model.Identifier()] = projectionReadPolicy{projection.KernelDefinition(), model, len(roots) != 0}
	}
	return func(ctx context.Context, model readmodels.Descriptor) (bool, error) {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		policy, known := policies[model.Identifier()]
		if !known {
			return false, nil
		}
		if policy.protected {
			return true, fmt.Errorf("%w: protected projection replay release", ErrUnsupported)
		}
		if model.EventSequence() != policy.model.EventSequence() || model.Schema() != policy.model.Schema() {
			return true, fmt.Errorf("%w: projection read policy does not match bound model", ErrUnsupported)
		}
		definition := policy.definition
		// Kernel 19.29.4 starts replay/history roots empty and cannot honor
		// defaults. Relationship/deferred key history has no faithful witness.
		if definition.InitialModelState != "{}" || len(definition.Join) != 0 || len(definition.RemovedWithJoin) != 0 || len(definition.Children) != 0 || len(definition.Nested) != 0 {
			return true, fmt.Errorf("%w: projection defaults or relationship replay", ErrUnsupported)
		}
		// GetEventTypes returns explicit IDs even when live subscription also
		// accepts ALL. An empty filter handles pure ALL; a nonempty filter loses
		// other event types. Never report the resulting incomplete history.
		if definition.SubscribesToAllEvents && (len(definition.From) != 0 || len(definition.FromEvery) != 0 || len(definition.RemovedWith) != 0 || definition.FromEventProperty != nil) {
			return true, fmt.Errorf("%w: mixed all-event projection replay", ErrUnsupported)
		}
		for _, from := range definition.From {
			if from.Value.Key != "" && from.Value.Key != "$eventSourceId" {
				return true, fmt.Errorf("%w: custom-key projection replay", ErrUnsupported)
			}
		}
		for _, removal := range definition.RemovedWith {
			if removal.Value.Key != "" && removal.Value.Key != "$eventSourceId" {
				return true, fmt.Errorf("%w: custom-key projection replay", ErrUnsupported)
			}
		}
		return true, nil
	}, nil
}
