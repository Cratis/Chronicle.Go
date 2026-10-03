// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

// DecodeContent applies C#'s generation selection, falling back to Content when
// no requested representation is supplied. It never treats JSON null as an event.
func DecodeContent(descriptor events.Descriptor, ec events.Context, content []byte, generations map[events.Generation]json.RawMessage) (events.Context, any, error) {
	if target := descriptor.Ref().Generation; target != ec.EventType.Generation {
		if generational, ok := generations[target]; ok {
			content = generational
			ec.EventType.Generation = target
		}
	}
	data := bytes.TrimSpace(content)
	if len(data) == 0 || data[0] != '{' {
		return ec, nil, faults.ErrProtocol
	}
	value := reflect.New(descriptor.GoType())
	if err := json.Unmarshal(data, value.Interface()); err != nil {
		return ec, nil, fmt.Errorf("%w: invalid event content", faults.ErrProtocol)
	}
	return ec, value.Interface(), nil
}
