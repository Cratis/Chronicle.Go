// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"slices"

	"github.com/cratis/chronicle.go/compliance"
)

// FreezeProtection resolves application providers against every admitted node,
// including unused derivatives, and returns owned declarations backed by immutable
// results. Naming rebinding and reconnect can reuse these without executing the
// application providers again. Providers are called once per declared target.
func (p *Plan) FreezeProtection(options ...compliance.Declaration) ([]compliance.Declaration, error) {
	result := slices.Clone(options)
	for i, declaration := range result {
		provider := declaration.Provider()
		if provider == nil {
			continue
		}
		resolved := map[compliance.Target]compliance.Classification{}
		resolve := func(target compliance.Target) error {
			if _, exists := resolved[target]; exists {
				return nil
			}
			metadata, err := invoke(func() (compliance.Classification, error) { return provider(target) })
			if err != nil {
				return providerFailure(err)
			}
			if err := validateClassification(metadata); err != nil {
				return err
			}
			resolved[target] = metadata
			return nil
		}
		if err := p.visitAll(func(n *node) error {
			if err := resolve(compliance.Target{Type: dereference(n.typ)}); err != nil {
				return err
			}
			for _, f := range n.fields {
				declaring := n.typ
				for _, index := range f.index[:len(f.index)-1] {
					declaring = dereference(declaring.Field(index).Type)
					if err := resolve(compliance.Target{Type: declaring}); err != nil {
						return err
					}
				}
				if err := resolve(compliance.Target{Type: f.value.typ, DeclaringType: declaring, Field: n.typ.FieldByIndex(f.index).Name}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		result[i] = compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
			metadata, ok := resolved[target]
			if !ok {
				return compliance.Classification{}, protectionError("classification target changed during rebinding")
			}
			return metadata, nil
		})
	}
	return result, nil
}
