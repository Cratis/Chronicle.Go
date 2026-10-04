// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func mappingFields(t *testing.T, name string, typ reflect.Type) []serialization.Field {
	t.Helper()
	codecs, err := serialization.NewCodecs(serialization.Enum(
		serialization.EnumMember[graphEnum]{Name: "Zero", Value: 0},
		serialization.EnumMember[graphEnum]{Name: "One", Value: 1},
	))
	if err != nil {
		t.Fatal(err)
	}
	shape := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: typ, Tag: reflect.StructTag(`json:"` + name + `"`)}})
	plan, err := serialization.CompileWith(shape, serialization.Config{Codecs: codecs})
	if err != nil {
		t.Fatal(err)
	}
	return plan.Fields()
}

func TestEnumFinalWritePathBoundary(t *testing.T) {
	enum := reflect.TypeFor[*graphEnum]()
	plain := reflect.TypeFor[*int32]()
	for _, tc := range []struct {
		name, target, source, expression string
		targetType, sourceType           reflect.Type
		kind                             expressionKind
		valid                            bool
	}{
		{"sparse safe", "Status", "", "Missing", enum, nil, pathExpression, true},
		{"sparse nested safe", "Status", "", "State.Value", enum, nil, pathExpression, true},
		{"sparse true", "Status", "", "true", enum, nil, pathExpression, false},
		{"sparse False", "Status", "", "False", enum, nil, pathExpression, false},
		{"sparse number", "Status", "", "1", enum, nil, pathExpression, false},
		{"sparse exponent", "Status", "", "1e2", enum, nil, pathExpression, false},
		{"sparse reserved", "Status", "", "$eventSourceId", enum, nil, pathExpression, false},
		{"sparse malformed", "Status", "", "State..Value", enum, nil, pathExpression, false},
		{"safe copy", "Status", "Status", "Status", enum, enum, pathExpression, true},
		{"raw dotted source", "Status", "State.Value", "State.Value", enum, enum, pathExpression, false},
		{"raw malformed source", "Status", "State-Value", "State-Value", enum, enum, pathExpression, false},
		{"raw numeric source", "Status", "42", "42", enum, enum, pathExpression, false},
		{"raw reserved source", "Status", "$Status", "$Status", enum, enum, pathExpression, false},
		{"raw indexed target", "State[0]", "Status", "Status", enum, enum, pathExpression, false},
		{"raw dotted target", "State.Value", "Status", "Status", enum, enum, pathExpression, false},
		{"source enum plain target", "State.Value", "State.Value", "State.Value", plain, enum, pathExpression, false},
		{"source enum missing target", "Status", "Status", "Status", plain, enum, pathExpression, false},
		{"raw dotted sparse target", "State.Value", "", "Missing", enum, nil, pathExpression, false},
		{"raw dotted literal target", "State.Value", "", "", enum, nil, literalExpression, false},
		{"raw dotted clear target", "State.Value", "", "", enum, nil, nullExpression, false},
		{"boolean named literal target", "true", "", "", enum, nil, literalExpression, true},
		{"ordinary dotted unchanged", "State.Value", "State.Value", "State.Value", plain, plain, pathExpression, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := mappingFields(t, tc.target, tc.targetType)
			var eventFields []serialization.Field
			if tc.sourceType != nil {
				eventFields = mappingFields(t, tc.source, tc.sourceType)
			}
			d := &definition{id: "final-path"}
			w := write{path: tc.target, expression: expression{kind: tc.kind, text: tc.expression}}
			err := validateEnumWrite(d, w, fields, eventFields, events.TypeRef{ID: "source", Generation: 1}, "every")
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var located *DeclarationError
			if !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &located) || located.Path != tc.target || located.GoField != "Value" || located.EventReference == "" {
				t.Fatalf("want located enum refusal: %v", err)
			}
		})
	}
}

type mappingNested struct{ Value int32 }
type mappingShape struct {
	Direct graphEnum `json:"State.Value"`
	State  *mappingNested
	Items  []mappingNested
}

func TestEnumRootInventoryUsesFieldOwnershipNotDots(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[graphEnum]{Name: "Zero", Value: 0}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := serialization.CompileWith(reflect.TypeFor[mappingShape](), serialization.Config{Codecs: codecs})
	if err != nil {
		t.Fatal(err)
	}
	fields := plan.Fields()
	var names []string
	for _, f := range enumRootFields(fields) {
		names = append(names, f.Name)
	}
	if !slices.Equal(names, []string{"State.Value", "State", "Items"}) {
		t.Fatalf("roots = %v", names)
	}
	if len(serialization.RootFields(fields)) != 2 {
		t.Fatal("generic RootFields semantics changed")
	}
	for _, field := range fields {
		if field.GoField == "State.Value" && !enumPropertySegments(field, fields) {
			t.Fatal("genuine nested ordinary path rejected")
		}
		if field.GoField == "Direct" && enumPropertySegments(field, fields) {
			t.Fatal("literal dotted root name admitted")
		}
	}
	// Both metadata entries render State.Value. The enum cannot disappear merely
	// because traversal order places the ordinary nested entry first.
	slices.Reverse(fields)
	if field, ok := enumMappingField(fields, "State.Value"); !ok || !field.IsEnum() {
		t.Fatal("ambiguous raw enum name hidden by nested field")
	}
}
