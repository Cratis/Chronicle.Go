// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"strings"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
)

type constraintComposition struct {
	name      string
	configure func(*constraints.Builder)
}

// ConfigureDeclaredConstraint explicitly composes a model-bound constraint with
// the existing builder (for example IgnoreCasing, scope, On or RemovedWith).
// It does not replace AddConstraint's duplicate protection. The callback runs
// once per NewClient registry snapshot, before connection work, using declaration
// paths (subsequently rebound through the client's naming policy); it must not perform I/O or mutate the registry. It may run
// concurrently across clients. Name must identify a derived constraint and cannot
// be changed by the callback. Duplicate composition registrations are rejected.
func (r *Registry) ConfigureDeclaredConstraint(name string, configure func(*constraints.Builder)) error {
	if r == nil || strings.TrimSpace(name) == "" || configure == nil {
		return constraintDeclarationError("constraint composition requires registry, name and callback")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, composition := range r.constraintCompositions {
		if composition.name == name {
			return constraintDeclarationError("duplicate constraint composition")
		}
	}
	r.constraintCompositions = append(r.constraintCompositions, constraintComposition{name: name, configure: configure})
	return nil
}

func compileDeclaredConstraints(catalog *events.Catalog, explicit []constraints.Definition, compositions []constraintComposition) ([]constraints.Definition, error) {
	derived, err := constraints.CompileDeclarations(catalog)
	if err != nil {
		return nil, err
	}
	for _, composition := range compositions {
		found := false
		for i, definition := range derived {
			if definition.Name() != composition.name {
				continue
			}
			found = true
			builder := definition.ToBuilder()
			composition.configure(builder)
			composed, err := builder.Build()
			if err != nil {
				return nil, &declarations.DeclarationError{Artifact: composition.name, Directive: "unique", Offset: -1, Message: "invalid constraint composition", Cause: err}
			}
			if composed.Name() != composition.name {
				return nil, constraintDeclarationError("composition cannot rename a constraint")
			}
			for _, event := range append(composed.EventTypes(), composed.RemovalTypes()...) {
				if !containsConstraintEvent(catalog.Descriptors(), event) {
					return nil, constraintDeclarationError("composition references an event outside this catalog")
				}
			}
			derived[i] = composed
		}
		if !found {
			return nil, constraintDeclarationError("composition references an unknown declared constraint")
		}
	}
	for _, definition := range derived {
		for _, existing := range explicit {
			if definition.Name() == existing.Name() {
				return nil, constraintDeclarationError("explicit and model-bound constraint names conflict; use ConfigureDeclaredConstraint for explicit composition")
			}
		}
	}
	return append(explicit, derived...), nil
}

func constraintDeclarationError(message string) error {
	return &declarations.DeclarationError{Artifact: "constraints", Directive: "unique", Offset: -1, Message: message, Cause: ErrInvalidConfiguration}
}
