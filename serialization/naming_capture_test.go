// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
)

type capturedNamingEvent struct {
	URLValue string
	ID       string
	Person   string
	Nested   capturedNamingDetail
}

type capturedNamingDetail struct {
	URLValue string
	Person   string
}

type capturedExplicitEvent struct {
	URLValue string                 `json:"url_value"`
	ID       string                 `json:"Id"`
	Nested   capturedExplicitDetail `json:"details"`
}

type capturedExplicitDetail struct {
	URLValue string `json:"uri"`
	Person   string `json:"display_name"`
}

type namingCaptureProfile struct {
	Policy          string
	DeclaredType    string
	Payload         json.RawMessage
	EventSchema     json.RawMessage
	ReadModelSchema json.RawMessage
	Fields          []struct {
		CLRPath        string
		Name           string
		SerializedPath string
		SourcePath     string
		TargetPath     string
	}
}

func TestNamingMatchesPackagedCSharpCapture(t *testing.T) {
	seen := map[string]bool{}
	for _, profile := range loadNamingCapture(t) {
		key := profile.Policy + "/" + profile.DeclaredType
		if seen[key] {
			t.Fatalf("duplicate capture profile %s", key)
		}
		seen[key] = true
		t.Run(key, func(t *testing.T) {
			t.Parallel()
			assertNamingProfile(t, profile)
		})
	}
	if len(seen) != 6 {
		t.Fatalf("capture profiles = %d, want both policies for all three types", len(seen))
	}
}

func loadNamingCapture(t *testing.T) []namingCaptureProfile {
	t.Helper()
	data, err := os.ReadFile("testdata/naming/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	var capture struct{ Profiles []namingCaptureProfile }
	if err := json.Unmarshal(data, &capture); err != nil {
		t.Fatal(err)
	}
	return capture.Profiles
}

func assertNamingProfile(t *testing.T, profile namingCaptureProfile) {
	t.Helper()
	value := namingCaptureValue(t, profile.DeclaredType)
	policy := namingCapturePolicy(t, profile.Policy)
	for _, boundary := range namingCaptureBoundaries(profile) {
		t.Run(boundary.name, func(t *testing.T) {
			plan, err := boundary.compile(reflect.TypeOf(value), policy)
			if err != nil {
				t.Fatal(err)
			}
			assertNamingPlan(t, plan, boundary.schema, profile)
		})
	}
}

type namingCaptureBoundary struct {
	name    string
	compile func(reflect.Type, ...serialization.NamingPolicy) (*serialization.Plan, error)
	schema  json.RawMessage
}

func namingCaptureBoundaries(profile namingCaptureProfile) []namingCaptureBoundary {
	event := namingCaptureBoundary{"event", serialization.Compile, profile.EventSchema}
	readModel := namingCaptureBoundary{"read-model", serialization.CompileReadModel, profile.ReadModelSchema}
	switch profile.DeclaredType {
	case "NamingEvent":
		return []namingCaptureBoundary{event}
	case "NamingReadModel":
		return []namingCaptureBoundary{readModel}
	default:
		return []namingCaptureBoundary{event, readModel}
	}
}

func assertNamingPlan(t *testing.T, plan *serialization.Plan, schema json.RawMessage, profile namingCaptureProfile) {
	t.Helper()
	payload, err := plan.Marshal(namingCaptureValue(t, profile.DeclaredType))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(namingJSON(t, payload), namingJSON(t, profile.Payload)) {
		t.Fatalf("payload = %s, C# = %s", payload, profile.Payload)
	}
	got, want := namingSchemaPaths(t, []byte(plan.Schema())), namingSchemaPaths(t, schema)
	if !slices.Equal(got, want) {
		t.Fatalf("schema property paths = %v, C# = %v", got, want)
	}
	assertCapturedNamingFields(t, plan, profile)
}

func namingCaptureValue(t *testing.T, name string) any {
	t.Helper()
	switch name {
	case "NamingEvent", "NamingReadModel":
		return capturedNamingEvent{"url", "identifier", "Ada", capturedNamingDetail{"nested-url", "Grace"}}
	case "ExplicitEvent":
		return capturedExplicitEvent{"url", "shared-identifier", capturedExplicitDetail{"nested-url", "Grace"}}
	default:
		t.Fatalf("unknown captured type %q", name)
		return nil
	}
}

func namingCapturePolicy(t *testing.T, name string) serialization.NamingPolicy {
	t.Helper()
	switch name {
	case "DefaultNamingPolicy":
		return serialization.PreservePropertyNames
	case "CamelCaseNamingPolicy":
		return serialization.CamelCase
	default:
		t.Fatalf("unknown captured policy %q", name)
		return 0
	}
}

func namingJSON(t *testing.T, data []byte) any {
	t.Helper()
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

// Compare only property names. Required lists, formats and nullability are not
// established by this naming witness (and are deliberately not normalized away).
func namingSchemaPaths(t *testing.T, data []byte) []string {
	t.Helper()
	var paths []string
	var visit func(map[string]any, string)
	visit = func(schema map[string]any, prefix string) {
		properties, _ := schema["properties"].(map[string]any)
		for name, child := range properties {
			path := prefix + name
			paths = append(paths, path)
			visit(child.(map[string]any), path+".")
		}
	}
	visit(namingJSON(t, data).(map[string]any), "")
	if len(paths) == 0 {
		t.Fatal("schema has no property names")
	}
	slices.Sort(paths)
	return paths
}

func assertCapturedNamingFields(t *testing.T, plan *serialization.Plan, profile namingCaptureProfile) {
	t.Helper()
	fields := plan.Fields()
	wantCount := 6
	if profile.DeclaredType == "ExplicitEvent" {
		wantCount = 5
	}
	if got := [2]int{len(fields), len(profile.Fields)}; got != [2]int{wantCount, wantCount} {
		t.Fatalf("field counts (Go, C#) = %v, want %d each", got, wantCount)
	}
	for _, expected := range profile.Fields {
		field, ok := serialization.FieldAt(fields, expected.SerializedPath)
		if !ok {
			t.Fatalf("missing field %q", expected.SerializedPath)
		}
		got := [3]string{field.Path, field.Name, field.GoField}
		want := [3]string{expected.SerializedPath, expected.Name, goNamingField(expected.CLRPath)}
		if got != want {
			t.Fatalf("field metadata = %v, want %v", got, want)
		}
		wantReflection := capturedReflectionPath(profile, expected.CLRPath, expected.SerializedPath)
		if got := [2]string{expected.SourcePath, expected.TargetPath}; got != [2]string{wantReflection, wantReflection} {
			t.Fatalf("packaged reflection paths for %s = %v, want %q", expected.CLRPath, got, wantReflection)
		}
	}
}

// Untagged fields agree with both packaged reflection-path roles. Tagged Go
// paths intentionally follow the serializer instead, unlike C#.
func capturedReflectionPath(profile namingCaptureProfile, clrPath, serializedPath string) string {
	if profile.DeclaredType != "ExplicitEvent" {
		return serializedPath
	}
	if profile.Policy == "CamelCaseNamingPolicy" {
		return strings.NewReplacer("Nested", "nested", "Person", "person", "Id", "id").Replace(clrPath)
	}
	return clrPath
}

// CompileReadModel's documented convention (serialization/plan.go, comment on
// CompileReadModel) translates only an untagged root Go ID to C# Id before the
// naming policy. A C# property actually declared ID does not undergo that step.
func TestUntaggedReadModelIDMatchesDocumentedDifference(t *testing.T) {
	for _, profile := range []struct{ policy, goName string }{
		{"DefaultNamingPolicy", "Id"},
		{"CamelCaseNamingPolicy", "id"},
	} {
		t.Run(profile.policy, func(t *testing.T) {
			t.Parallel()
			capture := findNamingCapture(t, profile.policy, "NamingEvent")
			assertReadModelIdentityDifference(t, capture, profile.goName)
		})
	}
}

func findNamingCapture(t *testing.T, policy, declaredType string) namingCaptureProfile {
	t.Helper()
	for _, profile := range loadNamingCapture(t) {
		if profile.Policy == policy && profile.DeclaredType == declaredType {
			return profile
		}
	}
	t.Fatalf("missing capture %s/%s", policy, declaredType)
	return namingCaptureProfile{}
}

func assertReadModelIdentityDifference(t *testing.T, capture namingCaptureProfile, goName string) {
	t.Helper()
	value := namingCaptureValue(t, capture.DeclaredType)
	plan, err := serialization.CompileReadModel(reflect.TypeOf(value), namingCapturePolicy(t, capture.Policy))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := plan.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	csharp := namingJSON(t, capture.Payload).(map[string]any)
	goPayload := namingJSON(t, payload).(map[string]any)
	assertNamingIdentityKey(t, csharp, "ID")
	assertNamingIdentityKey(t, goPayload, goName)
	if got := [2]any{csharp["ID"], goPayload[goName]}; got != [2]any{"identifier", "identifier"} {
		t.Fatalf("identity values = %v, want identifier on both sides", got)
	}
	delete(csharp, "ID")
	delete(goPayload, goName)
	if !reflect.DeepEqual(csharp, goPayload) {
		t.Fatalf("non-identity payload differs: C# = %v, Go = %v", csharp, goPayload)
	}
	assertNamingSchemaIdentity(t, capture.ReadModelSchema, "ID")
	assertNamingSchemaIdentity(t, []byte(plan.Schema()), goName)
	field, ok := serialization.FieldAt(plan.Fields(), goName)
	if !ok {
		t.Fatalf("missing Go identity field %q", goName)
	}
	if got := [3]string{field.Name, field.Path, field.GoField}; got != [3]string{goName, goName, "ID"} {
		t.Fatalf("Go identity metadata = %v", got)
	}
}

func assertNamingSchemaIdentity(t *testing.T, schema []byte, name string) {
	t.Helper()
	properties := namingJSON(t, schema).(map[string]any)["properties"].(map[string]any)
	assertNamingIdentityKey(t, properties, name)
}

func assertNamingIdentityKey(t *testing.T, object map[string]any, name string) {
	t.Helper()
	for _, candidate := range []string{"ID", "Id", "id"} {
		_, present := object[candidate]
		if present != (candidate == name) {
			t.Fatalf("identity key %q present = %t, want only %q in %v", candidate, present, name, object)
		}
	}
}

func goNamingField(clrPath string) string {
	if clrPath == "Id" {
		return "ID" // Root read-model convention or an explicit tag maps Go ID to C# Id.
	}
	return clrPath
}
