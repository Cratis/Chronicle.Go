// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"encoding/json"
	"strings"
	"testing"
)

type codecInspectionFailure struct{}

func (*codecInspectionFailure) Error() string { return "PRIVATE codec failure" }
func (*codecInspectionFailure) As(any) bool   { panic("PRIVATE error inspection") }

type inspectionFailureConcept string

func (v inspectionFailureConcept) ConceptValue() string         { return string(v) }
func (v inspectionFailureConcept) MarshalJSON() ([]byte, error) { return json.Marshal(string(v)) }
func (v inspectionFailureConcept) MarshalText() ([]byte, error) { return []byte(v), nil }
func (v *inspectionFailureConcept) UnmarshalText(b []byte) error {
	*v = inspectionFailureConcept(b)
	return nil
}
func (*inspectionFailureConcept) UnmarshalJSON([]byte) error { return &codecInspectionFailure{} }

func TestDecisionCodecBoundaryDoesNotInspectApplicationErrors(t *testing.T) {
	type model struct {
		Value inspectionFailureConcept `json:"value"`
	}
	for _, protected := range []bool{false, true} {
		var options []ModelOption
		if protected {
			options = append(options, WithPII("value"))
		}
		declaration, err := Define[model](options...)
		if err != nil {
			t.Fatal(err)
		}
		result, err := decodeDecision[model](Instance[json.RawMessage]{Value: json.RawMessage(`{"value":"text"}`), Exists: true}, declaration.Descriptor())
		if err == nil || result.Exists || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("unsafe codec result", err)
		}
	}
}
