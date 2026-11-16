// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type schemaBinaryModel struct {
	ID      []byte `json:"id"`
	Payload []byte `json:"payload"`
}

func TestModelSchemaValidationOrder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  modelConfig
		message string
		cause   error
	}{
		{"unknown index before binary identity", modelConfig{indexes: []string{"missing"}}, "unknown serialized property path: missing", faults.ErrInvalidConfiguration},
		{"duplicate index before binary index", modelConfig{indexes: []string{"payload", "payload"}}, "duplicate property path: payload", faults.ErrInvalidConfiguration},
		{"binary index before binary identity", modelConfig{indexes: []string{"payload"}}, "binary index is not supported: payload", faults.ErrUnsupported},
		{"binary identity before invalid subject", modelConfig{subject: "missing"}, "binary identity is not supported: id", faults.ErrUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := serialization.CompileReadModel(reflect.TypeFor[schemaBinaryModel]())
			if err != nil {
				t.Fatal(err)
			}
			_, err = modelSchema(plan, tc.config)
			want := fmt.Sprintf("%v: %s", tc.cause, tc.message)
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %s", err, want)
			}
		})
	}
}

func TestReservedPropertiesTraverseCollectionsAndMaps(t *testing.T) {
	for _, key := range []string{"items", "additionalProperties"} {
		t.Run(key, func(t *testing.T) {
			node := map[string]any{key: map[string]any{"properties": map[string]any{"__subjects": map[string]any{}}}}
			err := reservedProperties(node)
			want := fmt.Sprintf("%v: reserved compliance property: __subjects", faults.ErrInvalidConfiguration)
			if err == nil || err.Error() != want {
				t.Fatalf("error = %v, want %s", err, want)
			}
		})
	}
}
