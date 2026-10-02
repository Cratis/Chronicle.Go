// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/fundamentals.go/concepts"
)

func compileConcept(n *node, representation concepts.Representation, active map[reflect.Type]bool, policy NamingPolicy) (*node, error) {
	n.concept = &representation
	n.scalar = true
	format := ""
	switch representation.Kind {
	case concepts.KindUUID:
		format = "uuid"
	case concepts.KindDateOnly:
		format = "date"
	case concepts.KindTimeOnly:
		format = "time"
	case concepts.KindTimeSpan:
		format = "duration"
	default:
		underlying, err := compile(representation.Type, active, policy)
		if err != nil {
			return nil, err
		}
		n.schema = underlying.schema
		return n, nil
	}
	n.schema["type"], n.schema["format"] = "string", format
	return n, nil
}

func (n *node) encodeConcept(value reflect.Value, dictionary bool) (any, error) {
	// Call the declared codec exactly once, not ConceptValue on a fabricated value.
	data, err := json.Marshal(value.Interface())
	if err != nil {
		return nil, fmt.Errorf("%w: concept codec: %w", faults.ErrUnsupported, err)
	}
	if err := concepts.CheckJSON(*n.concept, data); err != nil {
		return nil, fmt.Errorf("%w: concept JSON: %w", faults.ErrUnsupported, err)
	}
	// Validate the actual wire integer rather than the concept's implementation
	// kind (a struct may wrap uint64; TimeSpan is int64 but writes a string).
	if n.schema["type"] == "integer" {
		underlying := reflect.New(n.concept.Type)
		if err := json.Unmarshal(data, underlying.Interface()); err != nil {
			return nil, fmt.Errorf("%w: concept integer: %w", faults.ErrUnsupported, err)
		}
		if err := n.checkInteger(underlying.Elem(), dictionary); err != nil {
			return nil, err
		}
	}
	return json.RawMessage(data), nil
}
