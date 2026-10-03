// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package declarations_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/declarations"
)

func TestEventDeclarationGrammarAndRoles(t *testing.T) {
	tag := `unique(name="email",message="a,b;\"c\"",sequences=["event-log", "outbox"]);subject`
	parsed, err := declarations.Parse(declarations.V1, tag)
	if err != nil {
		t.Fatal(err)
	}
	if err := declarations.Validate(declarations.Event, parsed); err != nil {
		t.Fatal(err)
	}
	if parsed[0].Args[2].Value.Kind != declarations.List || len(parsed[0].Args[2].Value.Args) != 2 || parsed[0].Args[1].Value.Text != `a,b;"c"` {
		t.Fatalf("%+v", parsed)
	}
	for _, tag := range []string{`unique`, `unique(sequences=[])`, `unique(message="")`, `subject`} {
		parsed, err := declarations.Parse(declarations.V1, tag)
		if err != nil {
			t.Fatal(err)
		}
		if err := declarations.Validate(declarations.Event, parsed); err != nil {
			t.Fatal(err)
		}
		if err := declarations.Validate(declarations.Model, parsed); err == nil {
			t.Fatal("event metadata accepted on model")
		}
	}
	parsed, err = declarations.Parse(declarations.V1, "index;key")
	if err != nil {
		t.Fatal(err)
	}
	if err := declarations.Validate(declarations.Model, parsed); err != nil {
		t.Fatal(err)
	}
	if err := declarations.Validate(declarations.Event, parsed); err == nil {
		t.Fatal("model metadata accepted on event")
	}
}

func FuzzEventDeclarations(f *testing.F) {
	for _, seed := range []string{`unique(name="email",sequences=["event-log","outbox"]);subject`, `unique(sequences=[])`, `unique(sequences=[[[]]])`, `pii`, `encrypted(scope=subject)`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, tag string) {
		first, err := declarations.Parse(declarations.V1, tag)
		second, again := declarations.Parse(declarations.V1, tag)
		if !reflect.DeepEqual(first, second) || (err == nil) != (again == nil) {
			t.Fatal("nondeterministic parse")
		}
		if err == nil {
			err = declarations.Validate(declarations.Event, first)
		}
		if err != nil {
			var declaration *declarations.DeclarationError
			if !errors.As(err, &declaration) || declaration.Offset < 0 || declaration.Offset > len(tag) {
				t.Fatalf("invalid provenance: %v", err)
			}
		}
	})
}

func TestInvalidEventDeclarationsAreTypedAndRedacted(t *testing.T) {
	for _, tag := range []string{`unique(name="")`, `unique(name=PRIVATE)`, `unique(message=12)`, `unique(sequences=[1])`, `unique(sequences=[" "])`, `unique(sequences="PRIVATE")`, `unique(sequences=["a",])`, `unique(sequences=["a")`, `unique(name="a",name="b")`, `unique(unknown="PRIVATE")`, `subject(value="PRIVATE")`, `pii`, `encrypted(scope=subject,details="PRIVATE")`, `unique(sequences=` + strings.Repeat("[", 40) + `"x"` + strings.Repeat("]", 40) + `)`} {
		parsed, err := declarations.Parse(declarations.V1, tag)
		if err == nil {
			err = declarations.Validate(declarations.Event, parsed)
		}
		var declaration *declarations.DeclarationError
		if !errors.As(err, &declaration) || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatalf("%s: %v", tag, err)
		}
	}
	for _, tag := range []string{`value(E,value=["x"])`, `children(E,key=value(["x"]))`} {
		parsed, err := declarations.Parse(declarations.V1, tag)
		if err == nil {
			err = declarations.Validate(declarations.Model, parsed)
		}
		if err == nil {
			t.Fatalf("list accepted as projection scalar: %s", tag)
		}
	}
}
