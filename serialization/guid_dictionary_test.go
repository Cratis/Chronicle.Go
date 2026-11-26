// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type guidAlias = uuid.UUID
type guidDictAlias = map[uuid.UUID]string
type guidDefined map[uuid.UUID]string
type namedString string

type guidKeyed struct{ Values map[uuid.UUID]string }
type guidAliased struct{ Values guidDictAlias }
type guidDefinedHolder struct{ Values guidDefined }

type guidNested struct {
	Outer map[string]guidKeyed
	Alias map[guidAlias]string
	Ptr   *guidKeyed
}

var (
	guidZero  = uuid.UUID{}
	guidA     = uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	guidMulti = uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	guidKnown = map[string]uuid.UUID{"zero": guidZero, "known": guidA}
)

func guidNaming(policy dictionaryPolicy) serialization.NamingPolicy {
	if policy.short == "camelCase" {
		return serialization.CamelCase
	}
	return serialization.PreservePropertyNames
}

func guidPlan[T any](t *testing.T, naming ...serialization.NamingPolicy) *serialization.Plan {
	t.Helper()
	plan, err := serialization.Compile(reflect.TypeFor[T](), naming...)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

// guidCases returns a finder over the immutable packaged capture.
func guidCases(t *testing.T) func(dictionaryPolicy, dictionaryRef) dictionaryCase {
	t.Helper()
	fixture, _ := loadDictionaryFixture(t)
	byID := map[string]dictionaryCase{}
	for _, profile := range fixture.Profiles {
		for _, c := range profile.Cases {
			byID[c.ID] = c
		}
	}
	return func(policy dictionaryPolicy, ref dictionaryRef) dictionaryCase {
		id := ref.id(policy)
		c, ok := byID[id]
		if !ok || c.Status != "ok" {
			t.Fatalf("fixture case %s missing or not ok", id)
		}
		return c
	}
}

func guidRef(input, stage, api string) dictionaryRef {
	return dictionaryRef{"GuidKeyed", input, stage, api}
}

type guidLookupResult struct {
	KeyCount int `json:"keyCount"`
	Lookups  []struct {
		Key   string  `json:"key"`
		Found bool    `json:"found"`
		Value *string `json:"value"`
	} `json:"lookups"`
}

// checkGuidLookup repeats the packaged lookup with independently built UUIDs.
func checkGuidLookup(t *testing.T, packaged dictionaryCase, got guidKeyed) {
	t.Helper()
	var want guidLookupResult
	if err := json.Unmarshal([]byte(*packaged.Output), &want); err != nil || want.KeyCount != len(got.Values) {
		t.Fatalf("%s: %v keys=%d/%d", packaged.ID, err, want.KeyCount, len(got.Values))
	}
	for _, lookup := range want.Lookups {
		value, found := got.Values[guidKnown[lookup.Key]]
		expected := ""
		if lookup.Value != nil {
			if err := json.Unmarshal([]byte(*lookup.Value), &expected); err != nil {
				t.Fatal(err)
			}
		}
		if found != lookup.Found || value != expected {
			t.Fatalf("%s %s: %v %q", packaged.ID, lookup.Key, found, value)
		}
	}
}

func jsonEqual(a, b []byte) bool {
	var left, right any
	return json.Unmarshal(a, &left) == nil && json.Unmarshal(b, &right) == nil && reflect.DeepEqual(left, right)
}

func checkGuidSchema(t *testing.T, policy dictionaryPolicy, plan *serialization.Plan, packaged dictionaryCase) {
	t.Helper()
	var ours, theirs struct{ Properties map[string]json.RawMessage }
	if json.Unmarshal([]byte(plan.Schema()), &ours) != nil || json.Unmarshal([]byte(*packaged.Output), &theirs) != nil {
		t.Fatalf("%s: unreadable schema", policy.short)
	}
	if len(ours.Properties) != 1 || !jsonEqual(ours.Properties[policy.root], theirs.Properties[policy.root]) {
		t.Fatalf("%s schema: %s vs %s", policy.short, plan.Schema(), *packaged.Output)
	}
}

func TestGuidDictionaryFollowsPackagedProfileForBothPolicies(t *testing.T) {
	find := guidCases(t)
	const value = "value:zero-and-nonzero"
	for _, policy := range dictionaryPolicies {
		plan := guidPlan[guidKeyed](t, guidNaming(policy))
		data, err := plan.Marshal(guidKeyed{Values: map[uuid.UUID]string{guidA: "nonzero", guidZero: "zero"}})
		if err != nil || string(data) != *find(policy, guidRef(value, "serialize", "")).Output {
			t.Fatalf("%s: %s %v", policy.short, data, err)
		}
		checkGuidSchema(t, policy, plan, find(policy, guidRef("type", "schema", "Generate")))
		for _, input := range []string{value, "raw:missing"} {
			var got guidKeyed
			if err := plan.Unmarshal([]byte(*find(policy, guidRef(input, "deserialize", "")).Input), &got); err != nil {
				t.Fatalf("%s %s: %v", policy.short, input, err)
			}
			checkGuidLookup(t, find(policy, guidRef(input, "lookup", "")), got)
			if input != value {
				continue
			}
			rewritten, err := plan.Marshal(got)
			if err != nil || string(rewritten) != *find(policy, guidRef(value, "rewrite", "")).Output {
				t.Fatalf("%s rewrite: %s %v", policy.short, rewritten, err)
			}
		}
	}
}

// Deliberate deviation: the packaged client keeps the last of two spellings of
// one Guid; Go refuses the document and leaves the destination untouched.
func TestGuidDictionaryRefusesPackagedSpellingCollisionForBothPolicies(t *testing.T) {
	find := guidCases(t)
	const collide = "raw:spellings-collide"
	for _, policy := range dictionaryPolicies {
		var packaged guidLookupResult
		if err := json.Unmarshal([]byte(*find(policy, guidRef(collide, "lookup", "")).Output), &packaged); err != nil || packaged.KeyCount != 1 {
			t.Fatalf("packaged precondition: %v %+v", err, packaged)
		}
		requireGuidRefusal(t, guidPlan[guidKeyed](t, guidNaming(policy)), *find(policy, guidRef(collide, "deserialize", "")).Input)
	}
}

// requireGuidRefusal checks a protocol error that leaves a populated destination
// unchanged; the baseline is cloned so aliasing cannot make the check vacuous.
func requireGuidRefusal(t *testing.T, plan *serialization.Plan, input string) {
	t.Helper()
	baseline := map[uuid.UUID]string{guidA: "keep"}
	target := guidKeyed{Values: maps.Clone(baseline)}
	err := plan.Unmarshal([]byte(input), &target)
	if !errors.Is(err, faults.ErrProtocol) || !maps.Equal(target.Values, baseline) {
		t.Fatalf("%v %+v", err, target)
	}
	if strings.Contains(err.Error(), "secret-value") {
		t.Fatalf("payload leaked: %v", err)
	}
}

// The packaged read-model schema generator accepts the type; Go keeps read
// models closed because projections assume string-compatible map keys.
func TestGuidDictionaryReadModelPlansStayClosedWhilePackagedSchemaSucceeds(t *testing.T) {
	find := guidCases(t)
	for _, policy := range dictionaryPolicies {
		find(policy, guidRef("type", "schema", "GenerateForReadModel"))
		for _, typ := range []reflect.Type{reflect.TypeFor[guidKeyed](), reflect.TypeFor[struct{ N guidNested }]()} {
			if _, err := serialization.CompileReadModel(typ, guidNaming(policy)); !errors.Is(err, faults.ErrUnsupported) {
				t.Fatalf("%s %v: %v", policy.short, typ, err)
			}
		}
	}
}

func TestGuidDictionaryRoundTripsAndAcceptsEitherHexCase(t *testing.T) {
	plan := guidPlan[guidNested](t)
	input := `{"Outer":{"k":{"Values":{"00112233-4455-6677-8899-AABBCCDDEEFF":"x"}}},"Alias":{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"y"},"Ptr":{"Values":{}}}`
	var got guidNested
	if err := plan.Unmarshal([]byte(input), &got); err != nil {
		t.Fatal(err)
	}
	if got.Outer["k"].Values[guidMulti] != "x" || got.Alias[guidA] != "y" || got.Ptr == nil || got.Ptr.Values == nil {
		t.Fatalf("%+v", got)
	}
	out, err := plan.Marshal(got)
	if err != nil || !strings.Contains(string(out), `"00112233-4455-6677-8899-aabbccddeeff":"x"`) {
		t.Fatalf("%s %v", out, err)
	}
}

func TestGuidDictionaryRefusesMalformedKeys(t *testing.T) {
	plan := guidPlan[guidKeyed](t)
	for name, key := range map[string]string{
		"short":    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa",
		"compact":  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"braces":   "{aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa}",
		"urn":      "urn:uuid:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		"space":    " aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa",
		"nonhex":   "gggggggg-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		"badsep":   "aaaaaaaa_aaaa-aaaa-aaaa-aaaaaaaaaaaa",
		"empty":    "",
		"longhyph": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaaa",
	} {
		t.Run(name, func(t *testing.T) {
			requireGuidRefusal(t, plan, `{"Values":{"`+key+`":"secret-value"}}`)
		})
	}
}

func TestGuidDictionaryRefusesSpellingCollisionsInEitherOrder(t *testing.T) {
	plan := guidPlan[guidKeyed](t)
	for _, input := range []string{
		`{"Values":{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"x","AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA":"y"}}`,
		`{"Values":{"AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA":"y","aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"x"}}`,
		`{"Values":{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"x","\u0061aaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"y"}}`,
	} {
		requireGuidRefusal(t, plan, input)
	}
}

// A late failure beneath a pointer leaves the destination pointer nil.
func TestGuidDictionaryNestedFailureLeavesPointerUnset(t *testing.T) {
	type holder struct{ P **guidKeyed }
	var late holder
	err := guidPlan[holder](t).Unmarshal([]byte(`{"P":{"Values":{"bad":"x"}}}`), &late)
	if !errors.Is(err, faults.ErrProtocol) || late.P != nil {
		t.Fatalf("%v %v", err, late.P)
	}
}

func TestGuidDictionaryNilAndNullDecodeLikeStringMaps(t *testing.T) {
	type stringKeyed struct{ Values map[string]string }
	gPlan, strPlan := guidPlan[guidKeyed](t), guidPlan[stringKeyed](t)
	for _, input := range []string{`{}`, `{"Values":null}`, `{"Values":{}}`} {
		var guids guidKeyed
		var strs stringKeyed
		if err := gPlan.Unmarshal([]byte(input), &guids); err != nil {
			t.Fatal(err)
		}
		if err := strPlan.Unmarshal([]byte(input), &strs); err != nil {
			t.Fatal(err)
		}
		if (guids.Values == nil) != (strs.Values == nil) || len(guids.Values) != len(strs.Values) {
			t.Fatalf("%s: %+v vs %+v", input, guids, strs)
		}
	}
	nilGuid, _ := gPlan.Marshal(guidKeyed{})
	nilString, _ := strPlan.Marshal(stringKeyed{})
	if string(nilGuid) != string(nilString) {
		t.Fatalf("%s vs %s", nilGuid, nilString)
	}
}

func TestGuidDictionaryAdmissionBoundary(t *testing.T) {
	type concept uuid.UUID
	for name, typ := range map[string]reflect.Type{
		"non-string value": reflect.TypeFor[map[uuid.UUID]int](),
		"pointer key":      reflect.TypeFor[map[*uuid.UUID]string](),
		"array key":        reflect.TypeFor[map[[16]byte]string](),
		"defined key":      reflect.TypeFor[map[concept]string](),
		"named string val": reflect.TypeFor[map[uuid.UUID]namedString](),
		"nested map value": reflect.TypeFor[map[uuid.UUID]map[string]string](),
		"defined map type": reflect.TypeFor[guidDefined](),
	} {
		wrapper := reflect.StructOf([]reflect.StructField{{Name: "V", Type: typ}})
		if _, err := serialization.Compile(wrapper); !errors.Is(err, faults.ErrUnsupported) && !errors.Is(err, faults.ErrInvalidConfiguration) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := serialization.Compile(reflect.TypeFor[guidDefinedHolder]()); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("defined map holder: %v", err)
	}
	var aliased guidAliased
	if err := guidPlan[guidAliased](t).Unmarshal([]byte(`{"Values":{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"y"}}`), &aliased); err != nil || aliased.Values[guidA] != "y" {
		t.Fatalf("true alias: %+v %v", aliased, err)
	}
}

func TestGuidDictionaryRebindKeepsOrRefusesKeys(t *testing.T) {
	from := guidPlan[guidKeyed](t)
	to, err := from.WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	renamed, err := from.RebindJSON([]byte(`{"Values":{"AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA":"x"}}`), to)
	if err != nil || string(renamed) != `{"values":{"AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA":"x"}}` {
		t.Fatalf("%s %v", renamed, err)
	}
	for _, input := range []string{
		`{"Values":{"not-a-guid":"x"}}`,
		`{"Values":{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa":"x","AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA":"y"}}`,
	} {
		if out, err := from.RebindJSON([]byte(input), to); out != nil || !errors.Is(err, faults.ErrProtocol) {
			t.Fatalf("%s %v", out, err)
		}
	}
}
