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

func compileConcept(n *node, representation concepts.Representation, state *compileState, policy NamingPolicy) (*node, error) {
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
		underlying, err := compile(representation.Type, state, policy, false)
		if err != nil {
			return nil, err
		}
		if hasBinary(underlying) {
			return nil, unsupported(n.typ, "binary concepts are not supported")
		}
		if hasEnum(underlying) {
			return nil, unsupported(n.typ, "enum concepts are not supported")
		}
		n.schema = underlying.schema
		return n, nil
	}
	n.schema["type"], n.schema["format"] = "string", format
	return n, nil
}

func (n *node) encodeConcept(value reflect.Value, dictionary bool, state *encodeState) (any, error) {
	// Call the declared codec exactly once, not ConceptValue on a fabricated value.
	data, err := invoke(func() ([]byte, error) {
		if state.privateCallbacks {
			// Go's JSON encoder may inspect a marshaler error (including errors.Is).
			// Intercept application failure before giving only successful JSON bytes
			// back to the normal scalar validation/JSON compaction path.
			codec, ok := value.Interface().(json.Marshaler)
			if !ok {
				return nil, errContent
			}
			data, err := codec.MarshalJSON()
			if err != nil {
				return nil, err
			}
			// A codec may reuse its buffer on the next field. Own successful
			// bytes before validation or any subsequent application callback.
			return append([]byte(nil), data...), nil
		}
		return json.Marshal(value.Interface())
	})
	if err != nil {
		if failure, ok := err.(*CallbackError); ok {
			state.panicked = failure.panicked
		}
		return nil, fmt.Errorf("%w: concept codec: %w", faults.ErrUnsupported, err)
	}
	if err := concepts.CheckJSON(*n.concept, data); err != nil {
		return nil, &CallbackError{cause: err}
	}
	// Validate the actual wire integer rather than the concept's implementation
	// kind (a struct may wrap uint64; TimeSpan is int64 but writes a string).
	if n.schema["type"] == "integer" {
		underlying := reflect.New(n.concept.Type)
		if err := json.Unmarshal(data, underlying.Interface()); err != nil {
			return nil, &CallbackError{cause: err}
		}
		if err := n.checkInteger(underlying.Elem(), dictionary); err != nil {
			return nil, err
		}
	}
	return json.RawMessage(data), nil
}
