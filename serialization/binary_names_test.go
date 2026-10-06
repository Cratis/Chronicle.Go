// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type binaryCrossLevel struct {
	Flat struct{ Value []byte } `json:"a.b"`
	A    struct {
		Alias string `json:"b.Value"`
	} `json:"a"`
}
type binaryCrossLevelReverse struct {
	A struct {
		Alias string `json:"b.Value"`
	} `json:"a"`
	Flat struct{ Value []byte } `json:"a.b"`
}
type binaryFunctionAlias struct {
	Alias   int `json:"Payload.Week"`
	Payload []byte
}
type binaryFunctionAliasReverse struct {
	Payload []byte
	Alias   int `json:"Payload.Week"`
}
type binaryCaseAlias struct {
	Note    string `json:"payload"`
	Payload []byte
}
type binaryCaseAliasReverse struct {
	Payload []byte
	Note    string `json:"payload"`
}
type binaryUnicodeAlias struct {
	Note    string `json:"é"`
	Payload []byte `json:"É"`
}
type binaryUnicodeAliasReverse struct {
	Payload []byte `json:"É"`
	Note    string `json:"é"`
}
type binaryNamesEmbedded struct {
	Alias string `json:"odd.name"`
}
type binaryNamesPromoted struct {
	*binaryNamesEmbedded
	Payload []byte
}
type binaryNamesVariant struct {
	Alias string `json:"odd.name"`
}
type binaryNamesFamilyOwner struct {
	Value   any
	Payload []byte
}

func TestBinaryNamesRefusePlan(t *testing.T) {
	cases := map[string]reflect.Type{
		"cross-level first":    reflect.TypeFor[binaryCrossLevel](),
		"cross-level last":     reflect.TypeFor[binaryCrossLevelReverse](),
		"function alias first": reflect.TypeFor[binaryFunctionAlias](),
		"function alias last":  reflect.TypeFor[binaryFunctionAliasReverse](),
		"case alias first":     reflect.TypeFor[binaryCaseAlias](),
		"case alias last":      reflect.TypeFor[binaryCaseAliasReverse](),
		"Unicode first":        reflect.TypeFor[binaryUnicodeAlias](),
		"Unicode last":         reflect.TypeFor[binaryUnicodeAliasReverse](),
		"promoted":             reflect.TypeFor[binaryNamesPromoted](),
		"nested":               reflect.TypeFor[struct{ Object binaryCrossLevel }](),
		"nested case":          reflect.TypeFor[struct{ Object binaryCaseAliasReverse }](),
		"promoted case":        reflect.TypeFor[struct{ binaryUnicodeAlias }](),
		"promoted function":    reflect.TypeFor[struct{ *binaryFunctionAliasReverse }](),
		"ordinary collection member": reflect.TypeFor[struct {
			Entries []struct {
				Alias string `json:"a.b"`
			}
			Payload []byte
		}](),
		"unrelated ordinary siblings": reflect.TypeFor[struct {
			Object struct {
				Name  string
				Alias string `json:"name"`
			}
			Payload []byte
		}](),
	}
	for _, name := range []string{"Week", "week", "WEEK", "$this", "*NotSet*", "true", "FALSE", "a.b", "[Payload]", "Call()", "a-b", "0Payload", "private-value-must-not-appear"} {
		cases["token "+name] = reflect.StructOf([]reflect.StructField{
			{Name: "Alias", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"` + name + `"`)},
			{Name: "Payload", Type: reflect.TypeFor[[]byte]()},
		})
	}
	for name, typ := range cases {
		t.Run(name, func(t *testing.T) {
			for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
				for _, compile := range []func(reflect.Type, ...serialization.NamingPolicy) (*serialization.Plan, error){serialization.Compile, serialization.CompileReadModel} {
					plan, err := compile(typ, policy)
					if plan != nil || !errors.Is(err, faults.ErrUnsupported) {
						t.Fatalf("plan = %v, error = %v, want ErrUnsupported", plan, err)
					}
					if strings.Contains(err.Error(), "private-value-must-not-appear") {
						t.Fatal("refusal exposed a property name")
					}
				}
			}
		})
	}
}

func TestBinaryNamesValidateDerivedVariantMembers(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[any, binaryNamesVariant]("variant"))
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		for _, typ := range []reflect.Type{reflect.TypeFor[binaryNamesFamilyOwner](), reflect.TypeFor[struct{ Payload []byte }]()} {
			plan, err := serialization.CompileWith(typ, serialization.Config{Codecs: codecs, NamingPolicy: policy})
			if plan != nil || !errors.Is(err, faults.ErrUnsupported) {
				t.Fatalf("derived member admitted: %v, %v", plan, err)
			}
		}
	}
	// The same variant graph without binary remains valid under every policy.
	plan, err := serialization.CompileWith(reflect.TypeFor[struct{ Value any }](), serialization.Config{Codecs: codecs})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plan.WithNamingPolicy(serialization.CamelCase); err != nil {
		t.Fatal(err)
	}
}

func TestBinaryFreeNamesKeepAdmissionAndRecompilation(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"cross-level": reflect.TypeFor[struct {
			Flat struct{ Value string } `json:"a.b"`
			A    struct {
				Alias string `json:"b.Value"`
			} `json:"a"`
		}](),
		"reserved function": reflect.TypeFor[struct {
			Week    string
			Alias   int `json:"Payload.Week"`
			Payload string
		}](),
		"case siblings": reflect.TypeFor[struct {
			Note    string `json:"payload"`
			Payload string `json:"Payload"`
		}](),
		"Unicode siblings": reflect.TypeFor[struct {
			Note    string `json:"é"`
			Payload string `json:"É"`
		}](),
		"case siblings reverse": reflect.TypeFor[struct {
			Payload string `json:"Payload"`
			Note    string `json:"payload"`
		}](),
		"function reverse": reflect.TypeFor[struct {
			Payload string
			Alias   int `json:"Payload.Week"`
		}](),
		"cross-level reverse": reflect.TypeFor[struct {
			A struct {
				Alias string `json:"b.Value"`
			} `json:"a"`
			Flat struct{ Value string } `json:"a.b"`
		}](),
	} {
		t.Run(name, func(t *testing.T) {
			plan, err := serialization.Compile(typ)
			if err != nil {
				t.Fatal(err)
			}
			for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
				next, err := plan.WithNamingPolicy(policy)
				if err != nil {
					t.Fatal(err)
				}
				value := reflect.New(typ).Elem().Interface()
				data, err := next.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := next.Unmarshal(data, reflect.New(typ).Interface()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestBinaryNamesKeepSimpleUnicodeAndRecompilation(t *testing.T) {
	type document struct {
		Payload    []byte
		Éclair     string
		Σ          string
		Underscore string `json:"_member2"`
		Kelvin     string `json:"K"`
		ASCII      string `json:"k"`
	}
	plan, err := serialization.Compile(reflect.TypeFor[document]())
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []serialization.NamingPolicy{serialization.CamelCase, serialization.LegacyGoCamelCase} {
		if _, err := plan.WithNamingPolicy(policy); err != nil {
			t.Fatal(err)
		}
	}
}
