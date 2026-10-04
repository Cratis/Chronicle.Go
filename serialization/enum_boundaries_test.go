// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/serialization"
)

type enumHook int32

// Panic rather than hiding a callback invocation behind a plausible result.
func (enumHook) String() string               { panic("enum String called") }
func (enumHook) MarshalJSON() ([]byte, error) { panic("enum MarshalJSON called") }
func (enumHook) MarshalText() ([]byte, error) { panic("enum MarshalText called") }
func (*enumHook) UnmarshalJSON([]byte) error  { panic("enum UnmarshalJSON called") }
func (*enumHook) UnmarshalText([]byte) error  { panic("enum UnmarshalText called") }
func (enumHook) IsZero() bool                 { panic("enum IsZero called") }

type enumHookDocument struct {
	Value    enumHook
	Optional *enumHook
	Values   []enumHook
}

type enumFamily interface{ member() }
type enumVariant struct{ Value enumSample }

func (enumVariant) member() {}

func TestEnumPlacementsFailClosed(t *testing.T) {
	for name, typ := range map[string]reflect.Type{
		"nested object":    reflect.TypeFor[struct{ Child struct{ Value enumSample } }](),
		"nested pointer":   reflect.TypeFor[struct{ Value **enumSample }](),
		"nested slice":     reflect.TypeFor[struct{ Value [][]enumSample }](),
		"nullable slice":   reflect.TypeFor[struct{ Value *[]enumSample }](),
		"nullable element": reflect.TypeFor[struct{ Value []*enumSample }](),
		"fixed array":      reflect.TypeFor[struct{ Value [2]enumSample }](),
		"map value":        reflect.TypeFor[struct{ Value map[string]enumSample }](),
		"map key":          reflect.TypeFor[struct{ Value map[enumSample]string }](),
		"omission": reflect.TypeFor[struct {
			Value enumSample `json:",omitempty"`
		}](),
		"zero omission": reflect.TypeFor[struct {
			Value enumSample `json:",omitzero"`
		}](),
		"pii": reflect.TypeFor[struct {
			Value enumSample `chronicle:"pii"`
		}](),
		"encrypted": reflect.TypeFor[struct {
			Value enumSample `chronicle:"encrypted"`
		}](),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := serialization.CompileWith(typ, serialization.Config{Codecs: enumCodecs(t)}); err == nil {
				t.Fatal("unsupported placement admitted")
			}
		})
	}
	codecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[enumSample]{Name: "One", Value: 1}), serialization.Derived[enumFamily, enumVariant]("variant"))
	if err != nil {
		t.Fatal(err)
	}
	// Unused family registrations are still audited, not mistaken for enums.
	if _, err := serialization.CompileWith(reflect.TypeFor[struct{ Plain int }](), serialization.Config{Codecs: codecs}); err == nil {
		t.Fatal("unused enum-bearing derivative admitted")
	}
	p := enumPlan[enumDocument](t)
	for _, option := range []compliance.Declaration{
		compliance.Property("Value", compliance.Classification{PII: true}),
		compliance.Property("Values", compliance.Classification{Encrypted: true}),
	} {
		if _, err := p.ProtectedSchema(option); err == nil {
			t.Fatal("classified enum admitted")
		}
	}
}

func TestEnumHooksAreNeverCalled(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[enumHook]{Name: "Zero", Value: 0}, serialization.EnumMember[enumHook]{Name: "One", Value: 1}))
	if err != nil {
		t.Fatal(err)
	}
	p, err := serialization.CompileWith(reflect.TypeFor[enumHookDocument](), serialization.Config{Codecs: codecs})
	if err != nil {
		t.Fatal(err)
	}
	one := enumHook(1)
	value := enumHookDocument{Value: 1, Optional: &one, Values: []enumHook{0, 1}}
	data, err := p.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded enumHookDocument
	if err := p.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(value, decoded) {
		t.Fatal("hooks changed representation")
	}
	if err := p.Unmarshal([]byte(`{"Value":"One","Optional":null,"Values":["Zero"]}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if err := p.Unmarshal([]byte(`{"Value":null}`), &decoded); err == nil {
		t.Fatal("null scalar admitted")
	}
	for _, f := range p.Fields() {
		if !f.IsEnum() {
			t.Fatal("enum metadata lost")
		}
		if _, err := f.Marshal(reflect.ValueOf(value).FieldByIndex(f.Index).Interface()); err != nil {
			t.Fatal(err)
		}
	}
	next, err := p.WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.RebindJSON(data, next); err != nil {
		t.Fatal(err)
	}
}

func TestEnumDeclarationAndRepresentationAreFrozen(t *testing.T) {
	members := []serialization.EnumMember[enumSample]{{Name: "Zero", Value: 0}, {Name: "One", Value: 1}}
	declaration := serialization.Enum(members...)
	members[1].Value, members[1].Name = 9, "Changed"
	codecs, err := serialization.NewCodecs(declaration)
	if err != nil {
		t.Fatal(err)
	}
	p, err := serialization.CompileWith(reflect.TypeFor[struct {
		Value    enumSample
		Optional *enumSample
		Values   []enumSample
	}](), serialization.Config{Codecs: codecs})
	if err != nil {
		t.Fatal(err)
	}
	field := p.Fields()[0]
	if _, err := field.Marshal(enumSample(1)); err != nil {
		t.Fatal("caller mutation changed table", err)
	}
	for _, other := range []serialization.Codec{
		serialization.Enum(serialization.EnumMember[enumSample]{Name: "Zero", Value: 0}, serialization.EnumMember[enumSample]{Name: "One", Value: 9}),
		serialization.Enum(serialization.EnumMember[enumSample]{Name: "Zero", Value: 0}, serialization.EnumMember[enumSample]{Name: "Renamed", Value: 1}),
		serialization.Flags(serialization.EnumMember[enumSample]{Name: "Zero", Value: 0}, serialization.EnumMember[enumSample]{Name: "One", Value: 1}),
	} {
		different, err := serialization.NewCodecs(other)
		if err != nil {
			t.Fatal(err)
		}
		next, err := serialization.CompileWith(reflect.TypeFor[struct {
			Value    enumSample
			Optional *enumSample
			Values   []enumSample
		}](), serialization.Config{Codecs: different})
		if err != nil {
			t.Fatal(err)
		}
		for i, f := range p.Fields() {
			if f.SameRepresentation(next.Fields()[i]) {
				t.Fatal("changed enum representation considered identical")
			}
		}
		for _, snapshot := range []string{`{}`, `{"Value":1}`, `{"Optional":null}`, `{"Values":[]}`} {
			if _, err := p.RebindJSON([]byte(snapshot), next); !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("reinterpretation: %v", err)
			}
		}
	}
	// Plain named Int32 is a different codec, even though its payload is numeric.
	plain, err := serialization.Compile(reflect.TypeFor[struct {
		Value    enumSample
		Optional *enumSample
		Values   []enumSample
	}]())
	if err != nil {
		t.Fatal(err)
	}
	if field.SameRepresentation(plain.Fields()[0]) {
		t.Fatal("enum equals plain integer")
	}
	if _, err := p.RebindJSON([]byte(`{}`), plain); err == nil {
		t.Fatal("enum metadata stripped")
	}
	if _, err := plain.RebindJSON([]byte(`{}`), p); err == nil {
		t.Fatal("enum metadata invented")
	}
}
