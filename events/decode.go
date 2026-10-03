// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"fmt"
	"reflect"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Decode selects T's registered generation from the kernel's alternate content,
// or uses Content when no alternate is supplied (C#'s raw fallback). It does not
// execute migrations locally or relabel the persisted context. T must be a
// registered non-pointer event struct. Use a Client.Catalogs/EventStore catalog
// to retain the selected naming policy. An unrelated ID is ErrProtocol.
func Decode[T any](catalog *Catalog, appended Appended) (T, error) {
	var result T
	if catalog == nil {
		return result, fmt.Errorf("%w: catalog required", faults.ErrInvalidConfiguration)
	}
	descriptor, ok := catalog.types[reflect.TypeFor[T]()]
	if !ok {
		return result, fmt.Errorf("%w: event type is not registered", faults.ErrInvalidConfiguration)
	}
	if descriptor.ref.ID != appended.Context.EventType.ID {
		return result, fmt.Errorf("%w: event identity mismatch", faults.ErrProtocol)
	}
	content := appended.Content
	if descriptor.ref.Generation != appended.Context.EventType.Generation {
		if alternate, ok := appended.GenerationalContent[descriptor.ref.Generation]; ok {
			content = alternate
		}
	}
	err := descriptor.Unmarshal(content, &result)
	return result, err
}

// Decode selects the exact delivered generation's codec and returns a pointer to
// its Go event type. An unregistered generation fails explicitly rather than
// guessing the latest codec; use the generic Decode to request a known shape.
func (a Appended) Decode(catalog *Catalog) (any, error) {
	if catalog == nil {
		return nil, fmt.Errorf("%w: catalog required", faults.ErrInvalidConfiguration)
	}
	descriptor, ok := catalog.LookupRef(a.Context.EventType)
	if !ok {
		return nil, fmt.Errorf("%w: delivered generation is not registered", faults.ErrInvalidConfiguration)
	}
	value := reflect.New(descriptor.GoType()).Interface()
	if err := descriptor.Unmarshal(a.Content, value); err != nil {
		return nil, err
	}
	return value, nil
}
