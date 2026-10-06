// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type binaryCaptureFixture struct {
	Profile   string
	Runtime   struct{ Version string }
	Packages  struct{ Chronicle, Fundamentals string }
	Isolation *struct{ RegistryCalls *int }
	Profiles  []enumCaptureProfile
}

func loadBinaryCapture(t *testing.T) binaryCaptureFixture {
	t.Helper()
	data, err := os.ReadFile("testdata/binary/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := parseBinaryCapture(data)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func parseBinaryCapture(data []byte) (binaryCaptureFixture, error) {
	var f binaryCaptureFixture
	if err := json.Unmarshal(data, &f); err != nil {
		return f, err
	}
	if f.Profile != "chronicle-19.29.4_fundamentals-7.19.6_net-10.0.12" || f.Runtime.Version != "10.0.12" || f.Packages.Chronicle != "19.29.4" || f.Packages.Fundamentals != "7.19.6" || f.Isolation == nil || f.Isolation.RegistryCalls == nil || *f.Isolation.RegistryCalls != 0 || len(f.Profiles) != 2 {
		return f, fmt.Errorf("invalid binary capture pins or isolation")
	}
	for i, p := range f.Profiles {
		policy := []string{"Cratis.Serialization.DefaultNamingPolicy", "Cratis.Serialization.CamelCaseNamingPolicy"}[i]
		if p.NamingPolicy != policy || len(p.Cases) != 84 || len(p.Schemas) != 8 || len(p.EventOptions.Converters) != 15 || p.EventOptions.Converters[14] != "Cratis.Serialization.DerivedTypeJsonConverterFactory" || !reflect.DeepEqual(p.SchemaOptions.Converters, []string{"Cratis.Json.EnumerableConceptAsJsonConverterFactory", "Cratis.Json.ConceptAsJsonConverterFactory"}) {
			return f, fmt.Errorf("incomplete binary profile")
		}
		seen := map[string]bool{}
		for _, s := range p.Schemas {
			id := s.DeclaredType + "/" + s.Operation
			validType := s.DeclaredType == "BinaryEvent" || s.DeclaredType == "BinaryModel" || s.DeclaredType == "BinaryRecord" || s.DeclaredType == "BinaryMapControl"
			if !validType || (s.Operation != "Generate" && s.Operation != "GenerateForReadModel") || seen[id] {
				return f, fmt.Errorf("invalid schema identity")
			}
			seen[id] = true
			if err := checkEnumCaptureResult(id, s.Result, ""); err != nil {
				return f, err
			}
		}
		seen = map[string]bool{}
		for _, c := range p.Cases {
			id := c.Operation + "/" + c.SchemaAPI + "/" + c.ID
			if c.DeclaredType != "BinaryEvent" || c.ID == "" || !json.Valid(c.Input) || seen[id] {
				return f, fmt.Errorf("invalid binary case")
			}
			seen[id] = true
			secondary := ""
			switch c.Operation {
			case "EventSerializer.Deserialize":
				secondary = "reserialize"
			case "EventSerializer.Serialize":
			case "ExpandoObjectConverter.ToExpandoObject/ToJsonObject":
				secondary = "toJson"
			default:
				return f, fmt.Errorf("invalid binary operation")
			}
			if secondary == "toJson" {
				if c.SchemaAPI != "Generate" && c.SchemaAPI != "GenerateForReadModel" {
					return f, fmt.Errorf("unlinked binary schema")
				}
			} else if c.SchemaAPI != "" {
				return f, fmt.Errorf("unexpected binary schema API")
			}
			if c.Result.Status == "accepted" && (secondary == "reserialize" && c.Result.Reserialize == nil || secondary == "toJson" && c.Result.ToJSON == nil) {
				return f, fmt.Errorf("missing binary secondary outcome")
			}
			if err := checkEnumCaptureResult(id, c.Result, secondary); err != nil {
				return f, err
			}
		}
	}
	return f, nil
}

func TestBinaryCaptureSourcesRemainOptInAndPinned(t *testing.T) {
	loadBinaryCapture(t)
	data, err := os.ReadFile("testdata/binary/provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		SDK, Runtime, ChroniclePackageSource, FundamentalsPackageSource string
		SHA256                                                          map[string]string
	}
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	if p.SDK != "10.0.401" || p.Runtime != "10.0.12" || p.ChroniclePackageSource != "ae5e00a8abaa688138b2c2f689e2b4659cccb4fd" || p.FundamentalsPackageSource != "14037b1ff8346b7944ad48065b8f66feca80dab5" || len(p.SHA256) != 8 {
		t.Fatal("provenance pins changed")
	}
	for _, name := range []string{"profile.json", "capture/.gitignore", "capture/BinaryCapture.csproj", "capture/Declarations.cs", "capture/Isolation.cs", "capture/Program.cs", "capture/global.json", "capture/packages.lock.json"} {
		data, err := os.ReadFile(filepath.Join("testdata/binary", name))
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(data)) != p.SHA256[name] {
			t.Fatalf("capture source hash changed: %s", name)
		}
	}
	if p.SHA256["profile.json"] != "1b2550e75f5ef4316e54ab3e4b0853089f5169c3bedecc08d8772dac01af4605" {
		t.Fatal("packaged golden changed")
	}
}

func TestBinaryCaptureValidationRejectsIncompleteEvidence(t *testing.T) {
	data, err := os.ReadFile("testdata/binary/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(map[string]any){
		func(f map[string]any) { delete(f, "isolation") },
		func(f map[string]any) { f["isolation"] = map[string]any{"registryCalls": nil} },
		func(f map[string]any) { f["isolation"] = map[string]any{"registryCalls": 1} },
		func(f map[string]any) { f["profiles"].([]any)[0].(map[string]any)["schemas"] = []any{} },
		func(f map[string]any) {
			cases := f["profiles"].([]any)[0].(map[string]any)["cases"].([]any)
			cases[0] = cases[1]
		},
		func(f map[string]any) {
			c := f["profiles"].([]any)[0].(map[string]any)["cases"].([]any)[33].(map[string]any)
			delete(c["result"].(map[string]any), "reserialize")
		},
		func(f map[string]any) {
			c := f["profiles"].([]any)[0].(map[string]any)["cases"].([]any)[1].(map[string]any)
			c["result"].(map[string]any)["toJson"] = nil
		},
		func(f map[string]any) {
			c := f["profiles"].([]any)[0].(map[string]any)["cases"].([]any)[0].(map[string]any)
			c["result"].(map[string]any)["reserialize"] = nil
		},
	} {
		var f map[string]any
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatal(err)
		}
		mutate(f)
		changed, err := json.Marshal(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parseBinaryCapture(changed); err == nil {
			t.Fatal("incomplete capture accepted")
		}
	}
}
