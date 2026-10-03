// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cratis/chronicle.go/internal/faults"
)

// GetValue reads a registered model for a runtime-discovered Go parameter type.
// It uses the same collection normalization as Reader.Get. typ must name the
// model struct or *model. Missing models return a typed nil pointer; a value-type
// request fails with ErrNotRegistered rather than manufacturing present state.
// Most applications should prefer the statically typed Reader.Get API.
func (s *Service) GetValue(ctx context.Context, typ reflect.Type, key Key) (any, error) {
	descriptor, ok := s.catalog.LookupType(typ)
	if !ok {
		return nil, notRegistered()
	}
	raw, err := s.get(ctx, descriptor, key, "")
	if err != nil {
		return nil, err
	}
	if !raw.Exists {
		if typ.Kind() == reflect.Pointer {
			return reflect.Zero(typ).Interface(), nil
		}
		return nil, fmt.Errorf("%w: absent read model requires a pointer parameter", faults.ErrNotRegistered)
	}
	value, err := descriptor.Unmarshal(raw.Value)
	if err != nil {
		return nil, fmt.Errorf("%w: model document does not match declared type", faults.ErrProtocol)
	}
	if typ.Kind() == reflect.Pointer {
		return value, nil
	}
	return reflect.ValueOf(value).Elem().Interface(), nil
}
