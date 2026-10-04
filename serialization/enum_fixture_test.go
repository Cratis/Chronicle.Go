// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// These tests validate captured .NET evidence, not a Go enum codec or parity claim.
type enumCaptureResult struct {
	Status             string             `json:"status"`
	Output             json.RawMessage    `json:"output"`
	Category           string             `json:"category"`
	InnerCategory      *string            `json:"innerCategory"`
	Reserialize        *enumCaptureResult `json:"reserialize"`
	ToJSON             *enumCaptureResult `json:"toJson"`
	reserializePresent bool
	toJSONPresent      bool
}

// Pointer decoding alone conflates an absent field with explicit JSON null.
func (result *enumCaptureResult) UnmarshalJSON(data []byte) error {
	type plainResult enumCaptureResult
	var decoded plainResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields struct {
		Reserialize json.RawMessage `json:"reserialize"`
		ToJSON      json.RawMessage `json:"toJson"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*result = enumCaptureResult(decoded)
	result.reserializePresent = len(fields.Reserialize) != 0
	result.toJSONPresent = len(fields.ToJSON) != 0
	return nil
}

type enumCaptureCase struct {
	DeclaredType string            `json:"declaredType"`
	ID           string            `json:"id"`
	Operation    string            `json:"operation"`
	SchemaAPI    string            `json:"schemaAPI,omitempty"`
	Input        json.RawMessage   `json:"input"`
	Result       enumCaptureResult `json:"result"`
}

type enumCaptureSchema struct {
	DeclaredType string            `json:"declaredType"`
	Operation    string            `json:"operation"`
	Result       enumCaptureResult `json:"result"`
}

type enumCaptureProfile struct {
	NamingPolicy  string                        `json:"namingPolicy"`
	EventOptions  struct{ Converters []string } `json:"eventOptions"`
	SchemaOptions struct{ Converters []string } `json:"schemaOptions"`
	Schemas       []enumCaptureSchema           `json:"schemas"`
	Cases         []enumCaptureCase             `json:"cases"`
}

type enumCaptureFixture struct {
	Profile     string                                        `json:"profile"`
	GoAdmission string                                        `json:"goAdmission"`
	Runtime     struct{ Version string }                      `json:"runtime"`
	Isolation   *struct{ RegistryCalls *int }                 `json:"isolation"`
	Assemblies  []struct{ Name, InformationalVersion string } `json:"assemblies"`
	Enums       []struct {
		Name    string
		Members []struct{ Name, Numeric string }
	} `json:"enums"`
	Profiles []enumCaptureProfile `json:"profiles"`
}

func TestEnumPackageFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/enum/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := parseEnumCaptureFixture(data)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.Profile != "chronicle-19.29.4_fundamentals-7.19.6_net-10.0.12" || fixture.Runtime.Version != "10.0.12" || !strings.HasPrefix(fixture.GoAdmission, "not-implemented;") {
		t.Fatal("capture profile/isolation/admission changed")
	}
	if len(fixture.Assemblies) != 4 || len(fixture.Enums) != 8 || len(fixture.Profiles) != 2 {
		t.Fatal("incomplete capture")
	}
	for _, assembly := range fixture.Assemblies {
		if assembly.Name == "" || assembly.InformationalVersion == "" {
			t.Fatal("missing assembly identity")
		}
	}
	for _, enum := range fixture.Enums {
		if enum.Name == "Int32Sample" {
			var names, numbers []string
			for _, member := range enum.Members {
				names = append(names, member.Name)
				numbers = append(numbers, member.Numeric)
			}
			if !reflect.DeepEqual(names, []string{"Zero", "One", "Max", "Min", "Negative"}) || !reflect.DeepEqual(numbers, []string{"0", "1", "2147483647", "-2147483648", "-1"}) {
				t.Fatal("CLR unsigned member ordering changed")
			}
		}
	}
	for i, profile := range fixture.Profiles {
		t.Run(profile.NamingPolicy, func(t *testing.T) {
			key, arrayKey := "Value", "Values"
			policy := "Cratis.Serialization.DefaultNamingPolicy"
			if i == 1 {
				key, arrayKey, policy = "value", "values", "Cratis.Serialization.CamelCaseNamingPolicy"
			}
			if profile.NamingPolicy != policy || len(profile.Cases) != 924 || len(profile.Schemas) != 54 {
				t.Fatal("incomplete naming profile")
			}
			if len(profile.EventOptions.Converters) != 15 || profile.EventOptions.Converters[1] != "Cratis.Json.EnumConverterFactory" || profile.EventOptions.Converters[14] != "Cratis.Serialization.DerivedTypeJsonConverterFactory" {
				t.Fatal("event converter order changed")
			}
			if !reflect.DeepEqual(profile.SchemaOptions.Converters, []string{"Cratis.Json.EnumerableConceptAsJsonConverterFactory", "Cratis.Json.ConceptAsJsonConverterFactory"}) {
				t.Fatal("schema options changed")
			}
			cases := make(map[string]enumCaptureCase)
			for _, c := range profile.Cases {
				id := c.DeclaredType + "/" + c.Operation + "/" + c.SchemaAPI + "/" + c.ID
				if _, exists := cases[id]; exists {
					t.Fatalf("duplicate case %s", id)
				}
				cases[id] = c
				if c.DeclaredType == "" || c.ID == "" || !json.Valid(c.Input) {
					t.Fatalf("invalid case %s", id)
				}
			}
			find := func(typ, operation, api, id string) enumCaptureResult {
				t.Helper()
				c, ok := cases[typ+"/"+operation+"/"+api+"/"+id]
				if !ok {
					t.Fatalf("missing %s %s %s %s", typ, operation, api, id)
				}
				return c.Result
			}
			read := func(typ, id string) enumCaptureResult { return find(typ, "EventSerializer.Deserialize", "", id) }
			for _, id := range []string{"token:5", "token:7", "token:8", "token:null", "token:true", "token:[]", "token:{}", "token:1.0", "token:1e0", "token:2147483648"} {
				if read("Scalar<Bits>", id).Status != "error" {
					t.Fatalf("historical Bits accepted %s", id)
				}
			}
			assertEnumCaptureJSON(t, read("Scalar<Bits>", "token:\"A, C\"").Output, `{"Value":{"declaredType":"Bits","numeric":"5"}}`)
			assertEnumCaptureJSON(t, read("Scalar<AllBits>", "token:-1").Output, `{"Value":{"declaredType":"AllBits","numeric":"-1"}}`)
			assertEnumCaptureJSON(t, read("Scalar<NoZero>", "missing").Output, `{"Value":{"declaredType":"NoZero","numeric":"0"}}`)
			if read("Scalar<NoZero>", "token:0").Status != "error" {
				t.Fatal("missing conflated with numeric zero")
			}
			assertEnumCaptureJSON(t, read("Int32Defaults", "missing").Output, `{"Value":{"declaredType":"Int32Sample","numeric":"1"},"Optional":null,"Values":null}`)
			if read("Int32Defaults", "explicit-null-value").Status != "error" {
				t.Fatal("null conflated with constructor default")
			}
			for _, id := range []string{"missing", "null"} {
				assertEnumCaptureJSON(t, read("NullableScalar<Int32Sample>", id).Output, `{"Value":null}`)
				assertEnumCaptureJSON(t, read("ArrayValue<Int32Sample>", id).Output, `{"Values":[]}`)
			}
			write := find("Scalar<Int32Sample>", "EventSerializer.Serialize", "", "numeric:-1")
			assertEnumCaptureString(t, write.Output, `{"`+key+`":-1}`)
			for _, api := range []string{"Generate", "GenerateForReadModel"} {
				op := "ExpandoObjectConverter.ToExpandoObject/ToJsonObject"
				scalar := find("Scalar<Bits>", op, api, "token:5")
				assertEnumCaptureString(t, scalar.ToJSON.Output, `{"`+key+`":"None"}`)
				array := find("ArrayValue<Int32Sample>", op, api, "token:[3,5,7,8,-1]")
				assertEnumCaptureString(t, array.ToJSON.Output, `{"`+arrayKey+`":[null,null,null,null,"Negative"]}`)
			}
			for _, schema := range profile.Schemas {
				if schema.DeclaredType == "Scalar<Int32Sample>" {
					var root struct {
						Properties map[string]struct {
							Type, Format string
							Enum         []int32
							Names        []string `json:"x-enumNames"`
						}
					}
					if err := json.Unmarshal(schema.Result.Output, &root); err != nil {
						t.Fatal(err)
					}
					property := root.Properties[key]
					if property.Type != "integer" || property.Format != "" || !reflect.DeepEqual(property.Enum, []int32{0, 1, 2147483647, -2147483648, -1}) || !reflect.DeepEqual(property.Names, []string{"Zero", "One", "Max", "Min", "Negative"}) {
						t.Fatalf("schema enum metadata changed: %s", schema.Result.Output)
					}
				}
				if schema.DeclaredType == "ConceptDefaultControl" {
					var root struct{ Properties map[string]json.RawMessage }
					if err := json.Unmarshal(schema.Result.Output, &root); err != nil {
						t.Fatal(err)
					}
					if schema.Operation == "Generate" {
						assertEnumCaptureJSON(t, root.Properties[key], `{"default":null}`)
					} else if schema.Operation == "GenerateForReadModel" && !bytes.Contains(root.Properties[key], []byte(`"x-enumNames"`)) {
						t.Fatal("read-model default control was not restored")
					}
				}
			}
		})
	}
}

// parseEnumCaptureFixture validates evidence completeness, not whether captured
// package operations succeeded. Secondary failures are independent observations.
func parseEnumCaptureFixture(data []byte) (enumCaptureFixture, error) {
	var fixture enumCaptureFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		return fixture, fmt.Errorf("decode enum capture: %w", err)
	}
	if fixture.Isolation == nil || fixture.Isolation.RegistryCalls == nil || *fixture.Isolation.RegistryCalls != 0 {
		return fixture, fmt.Errorf("capture requires explicit isolation registryCalls zero")
	}
	if len(fixture.Profiles) != 2 {
		return fixture, fmt.Errorf("incomplete naming profiles")
	}
	// Exact declarations from capture/Declarations.cs and Program.cs; never infer
	// the required schema set from the fixture being checked.
	declaredTypes := map[string]bool{"Int32Defaults": true, "NamingControl": true, "ConceptDefaultControl": true}
	for _, enum := range []string{"Plain", "NoZero", "Bits", "ByteEnum", "UIntEnum", "LongEnum", "Int32Sample", "AllBits"} {
		for _, wrapper := range []string{"Scalar", "NullableScalar", "ArrayValue"} {
			declaredTypes[wrapper+"<"+enum+">"] = true
		}
	}
	for _, profile := range fixture.Profiles {
		if len(profile.Cases) != 924 || len(profile.Schemas) != 54 {
			return fixture, fmt.Errorf("incomplete naming profile %s", profile.NamingPolicy)
		}
		seenSchemas := make(map[string]bool)
		for _, schema := range profile.Schemas {
			id := schema.DeclaredType + "/" + schema.Operation
			if !declaredTypes[schema.DeclaredType] || (schema.Operation != "Generate" && schema.Operation != "GenerateForReadModel") {
				return fixture, fmt.Errorf("unexpected schema %s", id)
			}
			if seenSchemas[id] {
				return fixture, fmt.Errorf("duplicate schema %s", id)
			}
			seenSchemas[id] = true
			if err := checkEnumCaptureResult(id, schema.Result, ""); err != nil {
				return fixture, err
			}
		}
		for declaredType := range declaredTypes {
			for _, api := range []string{"Generate", "GenerateForReadModel"} {
				if !seenSchemas[declaredType+"/"+api] {
					return fixture, fmt.Errorf("missing schema %s/%s", declaredType, api)
				}
			}
		}
		seenCases := make(map[string]bool)
		for _, c := range profile.Cases {
			id := c.DeclaredType + "/" + c.Operation + "/" + c.SchemaAPI + "/" + c.ID
			if !declaredTypes[c.DeclaredType] || c.ID == "" || !json.Valid(c.Input) || seenCases[id] {
				return fixture, fmt.Errorf("invalid or duplicate case %s", id)
			}
			seenCases[id] = true
			secondary := ""
			switch c.Operation {
			case "EventSerializer.Deserialize", "EventSerializer.Serialize":
				if c.Operation == "EventSerializer.Deserialize" {
					secondary = "reserialize"
				}
				if c.SchemaAPI != "" {
					return fixture, fmt.Errorf("unexpected schemaAPI %s", id)
				}
				if c.Operation == "EventSerializer.Deserialize" && c.Result.Status == "accepted" && c.Result.Reserialize == nil {
					return fixture, fmt.Errorf("missing reserialize %s", id)
				}
			case "ExpandoObjectConverter.ToExpandoObject/ToJsonObject":
				secondary = "toJson"
				if !seenSchemas[c.DeclaredType+"/"+c.SchemaAPI] {
					return fixture, fmt.Errorf("unlinked schemaAPI %s", id)
				}
				if c.Result.Status == "accepted" && c.Result.ToJSON == nil {
					return fixture, fmt.Errorf("missing toJson %s", id)
				}
			default:
				return fixture, fmt.Errorf("unexpected operation %s", id)
			}
			if err := checkEnumCaptureResult(id, c.Result, secondary); err != nil {
				return fixture, err
			}
		}
	}
	return fixture, nil
}

func checkEnumCaptureResult(id string, result enumCaptureResult, secondary string) error {
	switch result.Status {
	case "accepted":
		if !json.Valid(result.Output) || result.Category != "" || result.InnerCategory != nil {
			return fmt.Errorf("invalid success %s", id)
		}
	case "error":
		if !strings.HasPrefix(result.Category, "System.") || len(result.Output) != 0 || result.reserializePresent || result.toJSONPresent {
			return fmt.Errorf("invalid error %s", id)
		}
	default:
		return fmt.Errorf("missing status %s", id)
	}
	if result.reserializePresent && secondary != "reserialize" {
		return fmt.Errorf("unexpected reserialize %s", id)
	}
	if result.toJSONPresent && secondary != "toJson" {
		return fmt.Errorf("unexpected toJson %s", id)
	}
	if result.Reserialize != nil {
		if err := checkEnumCaptureResult(id+"/reserialize", *result.Reserialize, ""); err != nil {
			return err
		}
	}
	if result.ToJSON != nil {
		if err := checkEnumCaptureResult(id+"/toJson", *result.ToJSON, ""); err != nil {
			return err
		}
	}
	return nil
}

func assertEnumCaptureJSON(t *testing.T, actual json.RawMessage, expected string) {
	t.Helper()
	var compact bytes.Buffer
	if err := json.Compact(&compact, actual); err != nil {
		t.Fatal(err)
	}
	if compact.String() != expected {
		t.Fatalf("capture got %s, want %s", compact.String(), expected)
	}
}

func assertEnumCaptureString(t *testing.T, actual json.RawMessage, expected string) {
	t.Helper()
	var value string
	if err := json.Unmarshal(actual, &value); err != nil {
		t.Fatal(err)
	}
	if value != expected {
		t.Fatalf("capture got %s, want %s", value, expected)
	}
}

func TestEnumCaptureSourcesRemainOptInAndPinned(t *testing.T) {
	for name, fragments := range map[string][]string{
		"EnumCapture.csproj": {`Version="[19.29.4]"`, `Version="[7.19.6]"`, `<RuntimeFrameworkVersion>10.0.12</RuntimeFrameworkVersion>`, `<RollForward>Disable</RollForward>`},
		"global.json":        {`"version": "10.0.401"`, `"rollForward": "disable"`},
		"Isolation.cs":       {`GetMethod("InitializeJsonSerializationOptions"`, `ctor.Invoke([new EmptyArtifacts()`, `new ChronicleOptions { AutoDiscoverAndRegister = false }`},
		"Program.cs":         {`serializer.Deserialize(type,`, `serializer.Serialize(value)`, `generator.Generate(type)`, `generator.GenerateForReadModel(type)`, `FileMode.CreateNew`, `ForbiddenRegistry.Calls != 0`},
	} {
		data, err := os.ReadFile(filepath.Join("testdata/enum/capture", name))
		if err != nil {
			t.Fatal(err)
		}
		for _, fragment := range fragments {
			if !bytes.Contains(data, []byte(fragment)) {
				t.Fatalf("%s missing capture contract %q", name, fragment)
			}
		}
	}
}
