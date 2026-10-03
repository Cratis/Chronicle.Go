// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"

	"github.com/cratis/chronicle.go/readmodels"
)

func (s *EventStore) validateProjectionReplay(ctx context.Context, model readmodels.Descriptor) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	for _, projection := range s.projectionSnapshot {
		if projection.Model().Identifier() != model.Identifier() {
			continue
		}
		definition := projection.KernelDefinition()
		// Kernel 19.29.4 starts each replay/history root empty. It cannot honor
		// defaults. Relationship/deferred key history has no faithful witness;
		// refuse it rather than silently omitting cross-source contributions.
		if definition.InitialModelState != "{}" || len(definition.Join) != 0 || len(definition.RemovedWithJoin) != 0 || len(definition.Children) != 0 || len(definition.Nested) != 0 {
			return true, fmt.Errorf("%w: projection defaults or relationship replay", ErrUnsupported)
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
	}
	return false, nil
}
