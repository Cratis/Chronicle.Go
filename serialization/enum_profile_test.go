// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/internal/enumfixtures"
	"github.com/cratis/chronicle.go/serialization"
)

func profileTypes() map[string]reflect.Type {
	types := map[string]reflect.Type{}
	addProfileTypes[enumfixtures.Plain](types, "Plain")
	addProfileTypes[enumfixtures.NoZero](types, "NoZero")
	addProfileTypes[enumfixtures.Bits](types, "Bits")
	addProfileTypes[enumfixtures.Int32Sample](types, "Int32Sample")
	addProfileTypes[enumfixtures.AllBits](types, "AllBits")
	return types
}
func addProfileTypes[T ~int32](types map[string]reflect.Type, name string) {
	types["Scalar<"+name+">"] = reflect.TypeFor[enumfixtures.Scalar[T]]()
	types["NullableScalar<"+name+">"] = reflect.TypeFor[enumfixtures.NullableScalar[T]]()
	types["ArrayValue<"+name+">"] = reflect.TypeFor[enumfixtures.ArrayValue[T]]()
}

type enumRoundtripCase struct {
	NamingPolicy string          `json:"namingPolicy"`
	DeclaredType string          `json:"declaredType"`
	ID           string          `json:"id"`
	Payload      string          `json:"payload"`
	Expected     json.RawMessage `json:"expected"`
}

// Exercise every read and independent write for the five admitted Int32
// declarations against their actual package observations, not a reconstructed
// converter oracle. Only declared membership narrows successful C# results.
func TestEnumPackagedProfileRegression(t *testing.T) {
	data, err := os.ReadFile("testdata/enum/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture, err := parseEnumCaptureFixture(data)
	if err != nil {
		t.Fatal(err)
	}
	codecs, err := enumfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	types := profileTypes()
	members := map[string]map[string]bool{}
	for _, e := range fixture.Enums {
		members[e.Name] = map[string]bool{}
		for _, m := range e.Members {
			members[e.Name][m.Numeric] = true
		}
	}
	var supplemental []enumRoundtripCase
	reads, writes, schemas, narrowed := 0, 0, 0, 0
	for _, profile := range fixture.Profiles {
		policy := serialization.PreservePropertyNames
		if strings.Contains(profile.NamingPolicy, "CamelCase") {
			policy = serialization.CamelCase
		}
		for _, capture := range profile.Cases {
			typ, exists := types[capture.DeclaredType]
			if !exists || !strings.HasPrefix(capture.Operation, "EventSerializer.") {
				continue
			}
			p, err := serialization.CompileWith(typ, serialization.Config{Codecs: codecs, NamingPolicy: policy})
			if err != nil {
				t.Fatal(err)
			}
			id := profile.NamingPolicy + "/" + capture.DeclaredType + "/" + capture.Operation + "/" + capture.ID
			t.Run(id, func(t *testing.T) {
				value := reflect.New(typ)
				if capture.Operation == "EventSerializer.Deserialize" {
					reads++
					var input string
					if err := json.Unmarshal(capture.Input, &input); err != nil {
						t.Fatal(err)
					}
					admit := capture.Result.Status == "accepted" && declaredSnapshot(capture.Result.Output, members)
					err := p.Unmarshal([]byte(input), value.Interface())
					if !admit {
						if capture.Result.Status == "accepted" {
							narrowed++
						}
						if err == nil {
							t.Fatal("unsafe captured value accepted")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					encoded, err := p.Marshal(value.Elem().Interface())
					if err != nil {
						t.Fatal(err)
					}
					var want string
					if capture.Result.Reserialize == nil || json.Unmarshal(capture.Result.Reserialize.Output, &want) != nil {
						t.Fatal("missing .NET reserialization")
					}
					if string(encoded) != want {
						t.Fatalf("Go %s, .NET %s", encoded, want)
					}
					supplemental = append(supplemental, enumRoundtripCase{profile.NamingPolicy, capture.DeclaredType, capture.ID, string(encoded), capture.Result.Output})
				} else {
					writes++
					populateEnumSnapshot(t, value.Elem(), capture.Input)
					encoded, err := p.Marshal(value.Elem().Interface())
					if !declaredSnapshot(capture.Input, members) {
						if err == nil {
							t.Fatal("undeclared independent C# write accepted")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					var want string
					if capture.Result.Status != "accepted" || json.Unmarshal(capture.Result.Output, &want) != nil {
						t.Fatal("invalid .NET write result")
					}
					if string(encoded) != want {
						t.Fatalf("Go %s, .NET %s", encoded, want)
					}
				}
			})
		}
		for _, capture := range profile.Schemas {
			typ, exists := types[capture.DeclaredType]
			if !exists {
				continue
			}
			schemas++
			compile := serialization.CompileWith
			if capture.Operation == "GenerateForReadModel" {
				compile = serialization.CompileReadModelWith
			}
			p, err := compile(typ, serialization.Config{Codecs: codecs, NamingPolicy: policy})
			if err != nil {
				t.Fatal(err)
			}
			var got, want map[string]any
			if json.Unmarshal([]byte(p.Schema()), &got) != nil || json.Unmarshal(capture.Result.Output, &want) != nil {
				t.Fatal("invalid schema")
			}
			// Go's root title and dialect identifier are independent of enum properties.
			delete(got, "$schema")
			delete(got, "title")
			delete(want, "title")
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s/%s: schema %s differs from %s", profile.NamingPolicy, capture.DeclaredType, p.Schema(), capture.Result.Output)
			}
		}
	}
	if reads < 700 || writes < 140 || schemas != 60 || narrowed == 0 || len(supplemental) < 100 {
		t.Fatalf("incomplete checks reads=%d writes=%d schemas=%d narrowed=%d roundtrips=%d", reads, writes, schemas, narrowed, len(supplemental))
	}
	t.Logf("packaged profile: %d reads, %d writes, %d schemas; %d deliberately narrowed reads; %d Go payloads for optional .NET roundtrip", reads, writes, schemas, narrowed, len(supplemental))
	if path := os.Getenv("CHRONICLE_ENUM_ROUNDTRIP_OUTPUT"); path != "" {
		data, err := json.MarshalIndent(supplemental, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.Write(data)
		closeErr := file.Close()
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
}

func declaredSnapshot(data json.RawMessage, members map[string]map[string]bool) bool {
	var value any
	if json.Unmarshal(data, &value) != nil {
		return false
	}
	var visit func(any) bool
	visit = func(v any) bool {
		switch v := v.(type) {
		case map[string]any:
			if typ, ok := v["declaredType"].(string); ok {
				numeric, ok := v["numeric"].(string)
				return ok && members[typ][numeric]
			}
			for _, child := range v {
				if !visit(child) {
					return false
				}
			}
		case []any:
			for _, child := range v {
				if !visit(child) {
					return false
				}
			}
		case nil:
		default:
			return false
		}
		return true
	}
	return visit(value)
}

func populateEnumSnapshot(t *testing.T, value reflect.Value, data json.RawMessage) {
	t.Helper()
	if string(data) == "null" {
		value.SetZero()
		return
	}
	switch value.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if err := json.Unmarshal(data, &object); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < value.NumField(); i++ {
			populateEnumSnapshot(t, value.Field(i), object[value.Type().Field(i).Name])
		}
	case reflect.Pointer:
		value.Set(reflect.New(value.Type().Elem()))
		populateEnumSnapshot(t, value.Elem(), data)
	case reflect.Slice:
		var items []json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			t.Fatal(err)
		}
		value.Set(reflect.MakeSlice(value.Type(), len(items), len(items)))
		for i, item := range items {
			populateEnumSnapshot(t, value.Index(i), item)
		}
	default:
		var snapshot struct{ Numeric string }
		if err := json.Unmarshal(data, &snapshot); err != nil {
			t.Fatal(err)
		}
		integer, err := strconv.ParseInt(snapshot.Numeric, 10, 32)
		if err != nil {
			t.Fatal(err)
		}
		value.SetInt(integer)
	}
}
