// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
)

// DefineGeneration builds a historical descriptor referring to a current event.
// Its ID cannot be overridden, and generation must be positive and lower than
// current. Options configure the historical shape; WithID and WithGeneration
// may only repeat the derived identity. Most callers use RegisterEventGeneration.
func DefineGeneration[Previous, Current any](current Type[Current], generation Generation, options ...TypeOption) (Type[Previous], error) {
	target := current.Descriptor()
	if target.typ == nil || target.historicalFor != nil || generation == 0 || generation >= target.ref.Generation {
		return Type[Previous]{}, fmt.Errorf("%w: historical generation requires a newer current event", faults.ErrInvalidConfiguration)
	}
	settings := append([]TypeOption{WithID(target.ref.ID), WithGeneration(generation)}, options...)
	previous, err := Define[Previous](settings...)
	if err != nil {
		return Type[Previous]{}, err
	}
	if previous.Ref() != (TypeRef{ID: target.ref.ID, Generation: generation}) {
		return Type[Previous]{}, fmt.Errorf("%w: historical identity cannot be overridden", faults.ErrInvalidConfiguration)
	}
	previous.descriptor.historicalFor = &target
	return previous, nil
}

// IsHistorical reports whether this descriptor is a generation of another type.
func (d Descriptor) IsHistorical() bool { return d.historicalFor != nil }
