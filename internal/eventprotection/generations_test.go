// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventprotection_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/eventprotection"
	"github.com/cratis/chronicle.go/internal/faults"
)

type schemaDescriptor struct{ id, schema string }

func (d schemaDescriptor) Schema() string { return d.schema }

func TestIdentityProtectionInspectsAllMatchingSchemas(t *testing.T) {
	plain := schemaDescriptor{"event", `{"type":"object"}`}
	protected := schemaDescriptor{"event", `{"type":"object","properties":{"value":{"type":"string","compliance":[{"metadataType":"PII"}]}}}`}
	invalid := schemaDescriptor{"event", `{`}
	for _, tc := range []struct {
		name        string
		descriptors []schemaDescriptor
		want        bool
		wantErr     error
	}{
		{"plain identity", []schemaDescriptor{plain, plain}, false, nil},
		{"protected sibling", []schemaDescriptor{plain, protected}, true, nil},
		{"unrelated protection", []schemaDescriptor{plain, {"other", protected.schema}}, false, nil},
		{"unrelated invalid schema", []schemaDescriptor{plain, {"other", invalid.schema}}, false, nil},
		{"invalid matching schema", []schemaDescriptor{plain, invalid}, false, faults.ErrInvalidConfiguration},
		{"invalid after protection", []schemaDescriptor{protected, invalid}, false, faults.ErrInvalidConfiguration},
		{"absent identity", nil, false, faults.ErrInvalidConfiguration},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := eventprotection.IdentityProtected(tc.descriptors, func(d schemaDescriptor) bool { return d.id == "event" })
			if actual != tc.want || !errors.Is(err, tc.wantErr) {
				t.Fatalf("protected = %v, error = %v", actual, err)
			}
		})
	}
}
