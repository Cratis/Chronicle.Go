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

var propertyNamingPolicies = []struct {
	name   string
	policy serialization.NamingPolicy
}{
	{"preserve", serialization.PreservePropertyNames},
	{"camelCase", serialization.CamelCase},
	{"legacy", serialization.LegacyGoCamelCase},
}

func TestPropertyNamesRefuseReservedTokens(t *testing.T) {
	// v19.29.4 DerivedPropertyFunctions.All contains only Week. The other
	// tokens belong to PropertyPath and LiteralExpressionResolver.
	for _, token := range []string{"Week", "$this", "*NotSet*", "true", "false"} {
		for _, spelling := range []string{token, strings.ToUpper(token), strings.ToLower(token)} {
			t.Run(spelling, func(t *testing.T) {
				typ := reflect.StructOf([]reflect.StructField{{
					Name: "Value", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(`json:"` + spelling + `"`),
				}})
				assertPropertyNameRefused(t, typ, spelling)
			})
		}
	}
	for _, typ := range []reflect.Type{
		reflect.TypeFor[struct{ Week int }](),
		reflect.TypeFor[struct{ True bool }](),
		reflect.TypeFor[struct{ False bool }](),
	} {
		t.Run("untagged/"+typ.Field(0).Name, func(t *testing.T) {
			assertPropertyNameRefused(t, typ, "")
		})
	}
}

func TestPropertyNamesRefuseCaseOnlySiblings(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"tagged": reflect.TypeFor[struct {
			Note    string `json:"payload"`
			Payload string `json:"Payload"`
		}](),
		"reversed": reflect.TypeFor[struct {
			Payload string `json:"Payload"`
			Note    string `json:"payload"`
		}](),
		"Unicode": reflect.TypeFor[struct {
			Left  string `json:"é"`
			Right string `json:"É"`
		}](),
	} {
		t.Run(name, func(t *testing.T) {
			assertPropertyNameRefused(t, typ, typ.Field(1).Tag.Get("json"))
		})
	}
}

func assertPropertyNameRefused(t *testing.T, typ reflect.Type, offendingName string) {
	t.Helper()
	for _, naming := range propertyNamingPolicies {
		t.Run(naming.name, func(t *testing.T) {
			name := offendingName
			if name == "" {
				name = simplePropertyTestName(typ.Field(0).Name, naming.policy)
			}
			nestedName := simplePropertyTestName("Nested", naming.policy)
			for _, shape := range []struct {
				name string
				typ  reflect.Type
				path string
			}{
				{"root", typ, name},
				{"nested", propertyNameWrapper(typ), nestedName + "." + name},
				{"pointer", propertyNameWrapper(reflect.PointerTo(typ)), nestedName + "." + name},
				{"collection", propertyNameWrapper(reflect.SliceOf(typ)), nestedName + "." + name},
				{"map value", propertyNameWrapper(reflect.MapOf(reflect.TypeFor[string](), typ)), nestedName + "." + name},
			} {
				t.Run(shape.name, func(t *testing.T) {
					for _, compile := range []func(reflect.Type, ...serialization.NamingPolicy) (*serialization.Plan, error){
						serialization.Compile, serialization.CompileReadModel,
					} {
						plan, err := compile(shape.typ, naming.policy)
						if plan != nil || !errors.Is(err, faults.ErrUnsupported) {
							t.Fatalf("plan = %v, error = %v, want ErrUnsupported", plan, err)
						}
						if !strings.Contains(err.Error(), `"`+shape.path+`"`) {
							t.Errorf("error = %v, want offending path %q", err, shape.path)
						}
					}
				})
			}
		})
	}
}

// These fixtures use ordinary title-case names, with no acronym special cases.
func simplePropertyTestName(name string, policy serialization.NamingPolicy) string {
	if policy == serialization.PreservePropertyNames {
		return name
	}
	return strings.ToLower(name[:1]) + name[1:]
}

type reservedNameVariant struct{ Week int }
type caseSiblingVariant struct {
	Note    string `json:"payload"`
	Payload string `json:"Payload"`
}

func TestPropertyNamesAuditUnusedDerivatives(t *testing.T) {
	for _, registration := range []serialization.Codec{
		serialization.Derived[any, reservedNameVariant]("reserved"),
		serialization.Derived[any, caseSiblingVariant]("siblings"),
	} {
		codecs, err := serialization.NewCodecs(registration)
		if err != nil {
			t.Fatal(err)
		}
		for _, naming := range propertyNamingPolicies {
			config := serialization.Config{Codecs: codecs, NamingPolicy: naming.policy}
			for _, compile := range []func(reflect.Type, serialization.Config) (*serialization.Plan, error){
				serialization.CompileWith, serialization.CompileReadModelWith,
			} {
				plan, err := compile(reflect.TypeFor[struct{ Ordinary string }](), config)
				if plan != nil || !errors.Is(err, faults.ErrUnsupported) {
					t.Fatalf("unused derivative admitted: plan = %v, error = %v", plan, err)
				}
			}
		}
	}
}

func propertyNameWrapper(typ reflect.Type) reflect.Type {
	return reflect.StructOf([]reflect.StructField{{Name: "Nested", Type: typ}})
}

func TestPropertyNamesKeepNonCollidingControls(t *testing.T) {
	type recursive struct {
		WeekNumber int
		Next       *recursive
	}
	for name, typ := range map[string]reflect.Type{
		"recursive": reflect.TypeFor[recursive](),
		"near reserved": reflect.TypeFor[struct {
			WeekNumber int
			TrueValue  bool
			FalseValue bool
		}](),
		"distinct siblings": reflect.TypeFor[struct {
			Note    string `json:"note"`
			Payload string
		}](),
		"separate levels": reflect.TypeFor[struct {
			Payload string `json:"payload"`
			Nested  struct{ Payload string }
		}](),
		"ordinary Unicode": reflect.TypeFor[struct {
			Note  string `json:"é"`
			Other string `json:"ö"`
		}](),
	} {
		t.Run(name, func(t *testing.T) {
			plan, err := serialization.Compile(typ)
			if err != nil {
				t.Fatal(err)
			}
			for _, naming := range propertyNamingPolicies {
				t.Run(naming.name, func(t *testing.T) {
					if _, err := plan.WithNamingPolicy(naming.policy); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestPropertyNamesTagAndUntaggedSiblingRefuse(t *testing.T) {
	typ := reflect.TypeFor[struct {
		Note    string `json:"payload"`
		Payload string
	}]()
	for _, naming := range propertyNamingPolicies {
		t.Run(naming.name, func(t *testing.T) {
			want := faults.ErrUnsupported
			if naming.policy != serialization.PreservePropertyNames {
				// The existing exact-duplicate diagnostic still takes precedence.
				want = faults.ErrInvalidConfiguration
			}
			if plan, err := serialization.Compile(typ, naming.policy); plan != nil || !errors.Is(err, want) {
				t.Fatalf("plan = %v, error = %v, want %v", plan, err, want)
			}
		})
	}
}
