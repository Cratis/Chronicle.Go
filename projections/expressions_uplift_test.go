// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/serialization"
)

func TestExpressionEncodingPreservesKernelSpellings(t *testing.T) {
	for _, test := range []struct {
		value expression
		want  string
	}{
		{expression{}, ""},
		{expression{kind: invalidExpression}, ""},
		{expression{kind: pathExpression, text: "value"}, "value"},
		{expression{kind: contextExpression, text: "subject"}, "$eventContext(subject)"},
		{expression{kind: sourceExpression}, "$eventSourceId"},
		{expression{kind: literalExpression, literalKind: declarations.String, text: "hello"}, "$value(hello)"},
		{expression{kind: literalExpression, literalKind: declarations.Number, text: "1.5"}, "$value(1.5)"},
		{expression{kind: literalExpression, literalKind: declarations.Boolean, text: "true"}, "$value(True)"},
		{expression{kind: literalExpression, literalKind: declarations.Boolean, text: "false"}, "$value(False)"},
		{expression{kind: literalExpression, literalKind: declarations.Boolean, text: "invalid"}, "$value(False)"},
		{expression{kind: nullExpression}, "$null"},
		{expression{kind: addExpression, text: "amount"}, "$add(amount)"},
		{expression{kind: subtractExpression, text: "amount"}, "$subtract(amount)"},
		{expression{kind: incrementExpression}, "$increment"},
		{expression{kind: decrementExpression}, "$decrement"},
		{expression{kind: countExpression}, "$count"},
		{expression{kind: causedByExpression, text: "Name"}, "$causedBy(Name)"},
		{expression{kind: removeExpression}, "$remove"},
		{expression{kind: compositeExpression, parts: []keyPart{{name: "id", expression: expression{kind: sourceExpression}}}}, "$composite(id=$eventSourceId)"},
		{expression{kind: pathExpression, text: "value", once: true}, "$setOnce(value)"},
	} {
		if got := test.value.encode(); got != test.want {
			t.Errorf("encode(%+v) = %q, want %q", test.value, got, test.want)
		}
	}
}

func TestExpressionValidationPreservesDiagnosticOrder(t *testing.T) {
	target := serialization.Field{Type: reflect.TypeFor[int32](), Scalar: serialization.Integer}
	for _, test := range []struct {
		value   expression
		message string
	}{
		{expression{kind: incrementExpression, once: true}, "set once requires an event property, context, event source, caused-by or non-null literal value"},
		{expression{kind: addExpression, text: "amount_delta"}, "arithmetic path cannot be represented by the kernel expression grammar"},
		{expression{kind: addExpression, text: "amount"}, "arithmetic requires a numeric event field"},
		{expression{kind: pathExpression, text: "missing"}, "unknown or unsupported event property path"},
		{expression{kind: contextExpression, text: "bad path"}, "invalid context path"},
		{expression{kind: literalExpression, literalKind: declarations.String, text: "!"}, "literal cannot be represented by the kernel expression grammar"},
		{expression{kind: literalExpression, literalKind: declarations.String, text: "text"}, "string literal does not match target scalar"},
		{expression{kind: nullExpression}, "null requires a supported nullable pointer"},
		{expression{kind: removeExpression}, "remove requires a dictionary entry target"},
		{expression{}, "incomplete or unsupported expression"},
	} {
		err := validateExpression(test.value, target, nil, nil, nil)
		if got, want := upliftErrorMessage(err), invalid(test.message).Error(); got != want {
			t.Errorf("validate(%+v) = %q, want %q", test.value, got, want)
		}
	}
}

func TestCorrelationKeyValidationPreservesCompositeRules(t *testing.T) {
	part := keyPart{name: "id", expression: expression{kind: sourceExpression}}
	for _, test := range []struct {
		value    expression
		optional bool
		err      error
	}{
		{expression{}, true, nil},
		{expression{}, false, invalid("invalid or unrepresentable correlation key")},
		{expression{kind: sourceExpression}, false, nil},
		{expression{kind: compositeExpression}, false, invalid("invalid or unrepresentable correlation key")},
		{expression{kind: compositeExpression, parts: []keyPart{part}}, false, nil},
		{expression{kind: compositeExpression, parts: []keyPart{part, part}}, false, invalid("invalid or duplicate composite key part")},
		{expression{kind: compositeExpression, parts: []keyPart{{name: "id", expression: expression{kind: compositeExpression, parts: []keyPart{part}}}}}, false, invalid("invalid or duplicate composite key part")},
		{expression{kind: literalExpression, literalKind: declarations.String}, false, invalid("invalid or unrepresentable correlation key")},
		{expression{kind: literalExpression, literalKind: declarations.String, text: "\U00010400"}, false, invalid("invalid or unrepresentable correlation key")},
	} {
		err := validateKey(test.value, nil, nil, test.optional)
		if got, want := upliftErrorMessage(err), upliftErrorMessage(test.err); got != want {
			t.Errorf("validate key(%+v) = %q, want %q", test.value, got, want)
		}
	}
}

func upliftErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
