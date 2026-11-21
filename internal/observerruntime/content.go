// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/eventprotection"
	"github.com/cratis/chronicle.go/internal/faults"
)

// DecodeContent applies C#'s generation selection, falling back to Content when
// no requested representation is supplied. A generation mismatch for an identity
// with any protected generation is refused with ErrUnsupported. The complete
// registered catalog is required for mismatches. JSON null is never an event.
func DecodeContent(catalog *events.Catalog, descriptor events.Descriptor, appended events.Appended) (events.Context, any, error) {
	ec := appended.Context
	data := bytes.TrimSpace(appended.Content)
	if len(data) == 0 || data[0] != '{' {
		return ec, nil, faults.ErrProtocol
	}
	ec, content, err := observerGenerationContent(catalog, descriptor, appended)
	if err != nil {
		return ec, nil, err
	}
	data = bytes.TrimSpace(content)
	value := reflect.New(descriptor.GoType())
	if err := descriptor.Unmarshal(data, value.Interface()); err != nil {
		return ec, nil, fmt.Errorf("%w: invalid event content", faults.ErrProtocol)
	}
	return ec, value.Interface(), nil
}

func observerGenerationContent(catalog *events.Catalog, descriptor events.Descriptor, appended events.Appended) (events.Context, []byte, error) {
	ec, content := appended.Context, appended.Content
	target := descriptor.Ref().Generation
	if target == ec.EventType.Generation {
		return ec, content, nil
	}
	if catalog == nil {
		return ec, nil, faults.ErrInvalidConfiguration
	}
	protected, err := eventprotection.IdentityProtected(catalog.Descriptors(), func(d events.Descriptor) bool { return d.Ref().ID == descriptor.Ref().ID })
	if err != nil {
		return ec, nil, err
	}
	// Only current content is released: Cratis/Chronicle#4623;
	// remove this guard with Cratis/Chronicle.Go#96.
	if protected {
		return ec, nil, fmt.Errorf("%w: protected non-current observer generation", faults.ErrUnsupported)
	}
	if generational, ok := appended.GenerationalContent[target]; ok {
		content = generational
		ec.EventType.Generation = target
	}
	return ec, content, nil
}
