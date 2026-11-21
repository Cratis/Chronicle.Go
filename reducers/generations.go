// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func (p *Plan) validateGenerations(catalog *events.Catalog) error {
	for _, ref := range p.ordered {
		current, _ := catalog.LookupID(ref.ID)
		if ref == current.Ref() {
			continue
		}
		if err := historicalGenerationError(catalog, ref.ID); err != nil {
			return p.declarationError("", p.descriptors[ref.ID].GoType(), err)
		}
	}
	return nil
}

func historicalGenerationError(catalog *events.Catalog, id events.TypeID) error {
	for _, descriptor := range catalog.Descriptors() {
		if descriptor.Ref().ID != id {
			continue
		}
		roots, err := serialization.ProtectionRoots(descriptor.Schema())
		if err != nil {
			return err
		}
		// Only current content is released: Cratis/Chronicle#4623;
		// remove this guard with Cratis/Chronicle.Go#96.
		if len(roots) > 0 {
			return fmt.Errorf("%w: protected historical observer generation", faults.ErrUnsupported)
		}
	}
	return nil
}
