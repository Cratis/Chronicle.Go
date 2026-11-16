// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"testing"

	projections "github.com/cratis/chronicle.go/contracts/projections"
)

func TestDecisionAdmissionChecksShapeBeforeInputsAndInputsInOrder(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*projections.ProjectionDefinition)
		want   DecisionReadRefusalReason
	}{
		{"sequence before join", func(p *projections.ProjectionDefinition) {
			p.EventSequenceId = "other"
			p.Join = []*projections.KeyValuePair_EventType_JoinDefinition{{}}
		}, DecisionNotEventLog},
		{"all key before malformed input", func(p *projections.ProjectionDefinition) {
			p.All.Key = "other"
			p.From[0] = nil
		}, DecisionNotEventSourceKeyed},
		{"event before key", func(p *projections.ProjectionDefinition) {
			p.From[0].Key.Id = "unknown"
			p.From[0].Value.Key = "other"
		}, DecisionUnsupportedEventType},
		{"from before removal", func(p *projections.ProjectionDefinition) {
			p.From[0].Value.ParentKey = "other"
			p.RemovedWith[0] = nil
		}, DecisionNotEventSourceKeyed},
		{"removal event before key", func(p *projections.ProjectionDefinition) {
			p.RemovedWith[0].Key = nil
			p.RemovedWith[0].Value.Key = "other"
		}, DecisionUnsupportedEventType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDecisionFixture(t)
			tc.change(f.catalog.Projections[0])
			if got := f.reader.Admit(); got.Reason != tc.want || got.IsAdmitted {
				t.Fatalf("admission=%+v, want %s", got, tc.want)
			}
		})
	}
}

func TestDecisionKeySchemaPreservesNullableShapeAndRejectsMalformedReferences(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema string
		shape  keyShape
		valid  bool
	}{
		{"null first", `{"properties":{"id":{"type":["null","string"],"format":"uuid"}}}`, keyShape{name: "id", format: "uuid", nullable: true}, true},
		{"invalid format", `{"properties":{"id":{"type":"string","format":42}}}`, keyShape{}, false},
		{"external ref", `{"properties":{"id":{"$ref":"other"}}}`, keyShape{}, false},
		{"missing ref", `{"properties":{"id":{"$ref":"#/definitions/missing"}}}`, keyShape{}, false},
		{"id precedes Id", `{"properties":{"id":{"type":"integer"},"Id":{"type":"string"}}}`, keyShape{name: "id"}, false},
		{"invalid JSON", `{`, keyShape{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shape, valid := decisionKeySchema(tc.schema)
			if shape != tc.shape || valid != tc.valid {
				t.Fatalf("shape=%+v valid=%v, want %+v %v", shape, valid, tc.shape, tc.valid)
			}
		})
	}
}
