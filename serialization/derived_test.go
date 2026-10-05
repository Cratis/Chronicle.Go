// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func derivedPlan[T any](t *testing.T, codecs *serialization.Codecs, policy serialization.NamingPolicy) *serialization.Plan {
	t.Helper()
	plan, err := serialization.CompileWith(reflect.TypeFor[T](), serialization.Config{Codecs: codecs, NamingPolicy: policy})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
func derivedCodecs(t *testing.T) *serialization.Codecs {
	t.Helper()
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	return codecs
}

func TestDerivedCapturedDotNETPayloadsAndOpenSchema(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy serialization.NamingPolicy
	}{
		{"DefaultNamingPolicy", serialization.PreservePropertyNames}, {"CamelCaseNamingPolicy", serialization.CamelCase},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := derivedPlan[derivedfixtures.MembersChanged](t, derivedCodecs(t), tc.policy)
			fixture, err := os.ReadFile("testdata/derived/" + tc.name + ".payload.json")
			if err != nil {
				t.Fatal(err)
			}
			data, err := plan.Marshal(derivedfixtures.Sample())
			if err != nil || !bytes.Equal(data, bytes.TrimSpace(fixture)) {
				t.Fatalf("payload = %s, error %v; want %s", data, err, fixture)
			}
			var decoded derivedfixtures.MembersChanged
			if err := plan.Unmarshal(fixture, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, derivedfixtures.Sample()) {
				t.Fatalf("round trip = %#v", decoded)
			}
			for _, producer := range []string{"go", "csharp"} {
				stored, err := os.ReadFile("testdata/derived/" + tc.name + "." + producer + ".kernel.json")
				if err != nil {
					t.Fatal(err)
				}
				if err := plan.Unmarshal(stored, &decoded); err != nil || !reflect.DeepEqual(decoded, derivedfixtures.Sample()) {
					t.Fatalf("captured kernel round trip %s: %#v %v", producer, decoded, err)
				}
			}
			capturedSchema, err := os.ReadFile("testdata/derived/" + tc.name + ".schema.json")
			if err != nil {
				t.Fatal(err)
			}
			var captured, generated map[string]any
			if err := json.Unmarshal(capturedSchema, &captured); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal([]byte(plan.Schema()), &generated); err != nil {
				t.Fatal(err)
			}
			primary, members := "Primary", "Members"
			if tc.policy == serialization.CamelCase {
				primary, members = "primary", "members"
			}
			for _, schema := range []map[string]any{captured, generated} {
				properties := schema["properties"].(map[string]any)
				if !reflect.DeepEqual(properties[primary], map[string]any{"type": "object"}) || !reflect.DeepEqual(properties[members].(map[string]any)["items"], map[string]any{"type": "object"}) {
					t.Fatal("family schema is not the captured open object", schema)
				}
			}
		})
	}
}

type derivedEnvelope struct {
	Value any `json:"value"`
}
type directFamilyAlias = any
type directVariant struct{ Child directFamilyAlias }
type pointerDirectVariant struct{ Child **directFamilyAlias }
type embeddedDirect struct{ Child any }
type promotedDirectVariant struct{ embeddedDirect }
type collisionVariant struct {
	Name string `json:"_derivedTypeId"`
}
type namingCollisionVariant struct {
	Name  string
	Other string `json:"name"`
}
type harmlessVariant struct{ Value string }
type taggedVariant struct {
	Label string `json:"explicit_label"`
}

func TestDerivedRegistrationValidationAndUnusedVariantAdmission(t *testing.T) {
	for name, registrations := range map[string][]serialization.Codec{
		"empty":         {{}},
		"noninterface":  {serialization.Derived[string, harmlessVariant]("x")},
		"nonassignable": {serialization.Derived[derivedfixtures.Member, harmlessVariant]("x")},
		"blank":         {serialization.Derived[any, harmlessVariant](" ")},
		"invalid UTF8":  {serialization.Derived[any, harmlessVariant](string([]byte{255}))},
		"duplicate":     {serialization.Derived[any, harmlessVariant]("x"), serialization.Derived[any, harmlessVariant]("x")},
		"id collision":  {serialization.Derived[any, harmlessVariant]("x"), serialization.Derived[any, taggedVariant]("x")},
		"pointer value": {serialization.Derived[any, harmlessVariant]("x"), serialization.Derived[any, *harmlessVariant]("y")},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := serialization.NewCodecs(registrations...); !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	for name, registration := range map[string]serialization.Codec{
		"direct alias":            serialization.Derived[any, directVariant]("sensitive-id"),
		"pointer layers":          serialization.Derived[any, pointerDirectVariant]("sensitive-id"),
		"promoted":                serialization.Derived[any, promotedDirectVariant]("sensitive-id"),
		"discriminator collision": serialization.Derived[any, collisionVariant]("sensitive-id"),
		"camelCase collision":     serialization.Derived[any, namingCollisionVariant]("sensitive-id"),
	} {
		t.Run(name, func(t *testing.T) {
			codecs, err := serialization.NewCodecs(registration)
			if err != nil {
				t.Fatal(err)
			}
			// The entire family is unused by the root. Admission must still audit it.
			_, err = serialization.CompileWith(reflect.TypeFor[harmlessVariant](), serialization.Config{Codecs: codecs})
			if !errors.Is(err, faults.ErrInvalidConfiguration) || strings.Contains(err.Error(), "sensitive-id") {
				t.Fatalf("error = %v", err)
			}
			if name != "camelCase collision" {
				var declaration *serialization.CodecError
				if !errors.As(err, &declaration) || declaration.Field == "" {
					t.Fatalf("missing typed field diagnostic: %v", err)
				}
			}
		})
	}
}

func TestDerivedExactDiscriminatorAndFailureAtomicDecode(t *testing.T) {
	plan := derivedPlan[derivedfixtures.MembersChanged](t, derivedCodecs(t), serialization.PreservePropertyNames)
	for _, member := range []string{
		`{}`, `{"_DerivedTypeId":"robot"}`, `{"_derivedTypeId":"unknown-private"}`, `{"_derivedTypeId":null}`, `{"_derivedTypeId":1}`,
		`{"_derivedTypeId":"robot","_derivedTypeId":"robot"}`, `{"_derivedTypeId":"robot","\u005fderivedTypeId":"human"}`,
		`[]`, `"private-value"`, `{"count":9007199254740993,"_derivedTypeId":"robot"}`,
	} {
		t.Run(member, func(t *testing.T) {
			before := derivedfixtures.Sample()
			value := before
			err := plan.Unmarshal([]byte(`{"Primary":`+member+`}`), &value)
			if !errors.Is(err, faults.ErrProtocol) || strings.Contains(err.Error(), "private") {
				t.Fatalf("error = %v", err)
			}
			if !reflect.DeepEqual(value, before) {
				t.Fatal("failed decode published partial state")
			}
		})
	}
	value := derivedfixtures.Sample()
	if err := plan.Unmarshal([]byte(`{"Primary":{"COUNT":7,"_derivedTypeId":"robot"}}`), &value); err != nil || value.Primary != (derivedfixtures.RobotValue{Count: 7}) {
		t.Fatalf("case-insensitive variant decode: %v %#v", err, value)
	}
}

func TestDerivedNilNumericMapAndCycleBoundaries(t *testing.T) {
	plan := derivedPlan[derivedfixtures.MembersChanged](t, derivedCodecs(t), serialization.PreservePropertyNames)
	for _, sample := range []derivedfixtures.MembersChanged{
		{Primary: (*derivedfixtures.HumanValue)(nil)},
		{Members: []derivedfixtures.Member{nil}},
		{Lookup: map[string]derivedfixtures.Member{"x": nil}},
		{Primary: derivedfixtures.RobotValue{Count: 1<<53 + 1}},
		{Primary: derivedfixtures.RobotValue{Count: -(1 << 53) - 1}},
		{Lookup: map[string]derivedfixtures.Member{"x": derivedfixtures.RobotValue{Count: 1<<53 + 1}}},
	} {
		if _, err := plan.Marshal(sample); err == nil {
			t.Fatalf("accepted invalid sample: %#v", sample)
		}
	}
	for _, number := range []int64{-(1 << 53), 0, 1 << 53} {
		if _, err := plan.Marshal(derivedfixtures.MembersChanged{Primary: derivedfixtures.RobotValue{Count: number}}); err != nil {
			t.Fatal(err)
		}
	}
	cycle := &derivedfixtures.HumanValue{}
	cycle.Children = []derivedfixtures.Member{cycle}
	if _, err := plan.Marshal(derivedfixtures.MembersChanged{Primary: cycle}); err == nil {
		t.Fatal("cycle accepted")
	}
	data, err := plan.Marshal(derivedfixtures.MembersChanged{})
	if err != nil || bytes.Contains(data, []byte(`"Primary"`)) {
		t.Fatalf("nil interface not omitted: %s %v", data, err)
	}
	type unsigned struct{ Number uint64 }
	codecs, err := serialization.NewCodecs(serialization.Derived[any, unsigned]("unsigned"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := derivedPlan[derivedEnvelope](t, codecs, 0).Marshal(derivedEnvelope{Value: unsigned{math.MaxUint64}}); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("unsigned guard: %v", err)
	}
	if _, err := plan.Marshal(derivedfixtures.MembersChanged{Primary: unregisteredMember{}}); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("unknown dynamic type: %v", err)
	}
}

// Use a fresh explicit interface in the unknown-implementation assertion below;
// another package cannot implement derivedfixtures.Member's private method.
type unregisteredMember struct{ derivedfixtures.Member }

func TestDerivedMetadataSnapshotsAndTagPolicy(t *testing.T) {
	plan := derivedPlan[derivedfixtures.MembersChanged](t, derivedCodecs(t), 0)
	field, ok := serialization.FieldAt(plan.Fields(), "Primary")
	if !ok {
		t.Fatal("missing family")
	}
	derivatives := field.Derivatives()
	if len(derivatives) != 2 || derivatives[0].Type != reflect.TypeFor[*derivedfixtures.HumanValue]() || derivatives[0].ID != "human" {
		t.Fatal(derivatives)
	}
	if _, ok := serialization.FieldAt(plan.Fields(), "Primary.name"); ok {
		t.Fatal("variant fields flattened")
	}
	if _, ok := serialization.FieldAt(derivatives[0].Fields(), "_derivedTypeId"); ok {
		t.Fatal("writable discriminator")
	}
	children, ok := serialization.FieldAt(derivatives[0].Fields(), "children")
	if !ok || len(children.Derivatives()) != 2 {
		t.Fatal("recursive family metadata missing")
	}
	fields := derivatives[0].Fields()
	fields[0].Name, fields[0].Index[0] = "changed", 999
	derivatives[0].ID = "changed"
	if field.Derivatives()[0].ID != "human" || field.Derivatives()[0].Fields()[0].Name != "name" {
		t.Fatal("metadata aliases plan")
	}
	codecs, err := serialization.NewCodecs(serialization.Derived[any, taggedVariant]("tag"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := derivedPlan[derivedEnvelope](t, codecs, 0).Marshal(derivedEnvelope{Value: taggedVariant{"tag"}})
	if err != nil || string(data) != `{"value":{"explicit_label":"tag","_derivedTypeId":"tag"}}` {
		t.Fatalf("Go explicit tag: %s %v", data, err)
	}
	captured, err := os.ReadFile("testdata/derived/DefaultNamingPolicy.unsafe-direct.payload.json")
	if err != nil || !bytes.Contains(captured, []byte(`"label":"tag","_derivedTypeId":"human"`)) {
		t.Fatal("missing actual C# ignored-name-override evidence", err)
	}
}

func TestDerivedRebindKeepsIdentityAndNestedPolicyWithoutCallbacks(t *testing.T) {
	plan := derivedPlan[derivedfixtures.MembersChanged](t, derivedCodecs(t), 0)
	next, err := plan.WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := os.ReadFile("testdata/derived/DefaultNamingPolicy.payload.json")
	if err != nil {
		t.Fatal(err)
	}
	got, err := plan.RebindJSON(fixture, next)
	if err != nil {
		t.Fatal(err)
	}
	var value derivedfixtures.MembersChanged
	if err := next.Unmarshal(got, &value); err != nil || !reflect.DeepEqual(value, derivedfixtures.Sample()) {
		t.Fatalf("rebind: %v %#v", err, value)
	}
	changed, err := serialization.NewCodecs(serialization.Derived[derivedfixtures.Member, *derivedfixtures.HumanValue]("renamed"), serialization.Derived[derivedfixtures.Member, derivedfixtures.RobotValue]("robot"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.RebindJSON(fixture, derivedPlan[derivedfixtures.MembersChanged](t, changed, 0)); err == nil {
		t.Fatal("representation change treated as rename")
	}
	if _, err := plan.RebindJSON([]byte(`{"Primary":{"_derivedTypeId":"robot","_derivedTypeId":"robot"}}`), next); err == nil {
		t.Fatal("duplicate snapshot discriminator accepted")
	}
}

type sensitiveVariant struct {
	Secret string `chronicle:"pii"`
}
type encryptedVariant struct {
	Secret string `chronicle:"encrypted"`
}
type roleVariant struct {
	Key string `chronicle:"key"`
}
type protectedSibling struct {
	Value  any
	Secret string `chronicle:"pii"`
}

func TestDerivedSecurityAuditsUnusedVariantsAndProtectedSiblings(t *testing.T) {
	for _, registration := range []serialization.Codec{serialization.Derived[any, sensitiveVariant]("pii"), serialization.Derived[any, encryptedVariant]("secret")} {
		codecs, err := serialization.NewCodecs(serialization.Derived[any, harmlessVariant]("safe"), registration)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := serialization.CompileWith(reflect.TypeFor[harmlessVariant](), serialization.Config{Codecs: codecs}); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("unused classified variant: %v", err)
		}
	}
	codecs, err := serialization.NewCodecs(serialization.Derived[any, harmlessVariant]("safe"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := serialization.CompileWith(reflect.TypeFor[protectedSibling](), serialization.Config{Codecs: codecs}); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("protected sibling: %v", err)
	}
	plan := derivedPlan[derivedEnvelope](t, codecs, 0)
	for _, declaration := range []compliance.Declaration{
		compliance.For[harmlessVariant](compliance.Classification{PII: true}),
		compliance.For[any](compliance.Classification{Encrypted: true}),
		compliance.Property("value", compliance.Classification{PII: true}),
		compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
			return compliance.Classification{PII: target.Type == reflect.TypeFor[harmlessVariant]()}, nil
		}),
	} {
		if _, err := plan.ProtectedSchema(declaration); !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("explicit classified family: %v", err)
		}
	}
	roleCodecs, err := serialization.NewCodecs(serialization.Derived[any, roleVariant]("role"))
	if err != nil {
		t.Fatal(err)
	}
	rolePlan := derivedPlan[derivedEnvelope](t, roleCodecs, 0)
	if err := rolePlan.ValidateRole(declarations.Event); err == nil {
		t.Fatal("unused role directive accepted")
	}
	type sourceVariant struct{ Source events.SourceID }
	sourceCodecs, err := serialization.NewCodecs(serialization.Derived[any, sourceVariant]("source"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := derivedPlan[derivedEnvelope](t, sourceCodecs, 0).ProtectedSchema(compliance.Property("value", compliance.Classification{PII: true})); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("protected source: %v", err)
	}
}

func TestDerivedUnusedFamilyAuditDoesNotReinterpretRootPropertyPaths(t *testing.T) {
	// The same type can be ordinary document data and an unused family variant.
	// Its root path override must not become variant-wide classification.
	codecs, err := serialization.NewCodecs(serialization.Derived[any, harmlessVariant]("safe"))
	if err != nil {
		t.Fatal(err)
	}
	plan := derivedPlan[harmlessVariant](t, codecs, serialization.CamelCase)
	schema, err := plan.ProtectedSchema(compliance.Property("value", compliance.Classification{PII: true}))
	if err != nil {
		t.Fatal("unrelated root classification affected unused family", err)
	}
	roots, err := serialization.ProtectionRoots(schema)
	if err != nil || !roots["value"] {
		t.Fatalf("ordinary protection lost: %v %v", roots, err)
	}
	// A type-wide classification still reaches and rejects that unused variant.
	if _, err := plan.ProtectedSchema(compliance.For[harmlessVariant](compliance.Classification{PII: true})); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("unused variant classification escaped audit: %v", err)
	}
}

func TestDerivedPlansAreConcurrentAndOptionsAreOwned(t *testing.T) {
	options := []serialization.Codec{serialization.Derived[derivedfixtures.Member, *derivedfixtures.HumanValue]("human"), serialization.Derived[derivedfixtures.Member, derivedfixtures.RobotValue]("robot")}
	codecs, err := serialization.NewCodecs(options...)
	if err != nil {
		t.Fatal(err)
	}
	options[0] = serialization.Codec{}
	plan := derivedPlan[derivedfixtures.MembersChanged](t, codecs, 0)
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			data, err := plan.Marshal(derivedfixtures.Sample())
			if err != nil {
				t.Error(err)
				return
			}
			var value derivedfixtures.MembersChanged
			if err := plan.Unmarshal(data, &value); err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(value, derivedfixtures.Sample()) {
				t.Error("concurrent round trip")
			}
		})
	}
	workers.Wait()
}
