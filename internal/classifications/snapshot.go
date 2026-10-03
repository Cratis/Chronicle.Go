// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package classifications freezes provider results by Go member identity, so
// later naming and producer binding cannot invoke application classifiers again.
package classifications

import (
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/internal/faults"
)

// Snapshot evaluates a schema with recording providers, then replaces each with
// a read-only lookup. Only the lookup is retained; application providers and
// mutable recording maps never escape into a published descriptor.
func Snapshot(declarations []compliance.Declaration, evaluate func([]compliance.Declaration) error) ([]compliance.Declaration, error) {
	result := slices.Clone(declarations)
	recorded := make(map[int]map[compliance.Target]compliance.Classification)
	for i, declaration := range result {
		provider := declaration.Provider()
		if provider == nil {
			continue
		}
		values := make(map[compliance.Target]compliance.Classification)
		recorded[i] = values
		result[i] = compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
			if value, ok := values[target]; ok {
				return value, nil
			}
			value, err := provider(target)
			if err == nil {
				values[target] = value
			}
			return value, err
		})
	}
	if err := evaluate(result); err != nil {
		return nil, err
	}
	for i, values := range recorded {
		result[i] = compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
			value, ok := values[target]
			if !ok {
				return value, fmt.Errorf("%w: classification target outside frozen schema", faults.ErrInvalidConfiguration)
			}
			return value, nil
		})
	}
	return result, nil
}
