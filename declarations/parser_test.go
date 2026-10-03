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

func TestTagGrammarPreservesScalarsReferencesAndOffsets(t *testing.T) {
	tag := ` key ; value(@opened,value="a,b;\"c\"");set(go("example.org/events.Opened"),from=details.Name);context(id("opened",2),from=occurred)`
	parsed, err := declarations.Parse(declarations.V1, tag)
	if err != nil {
		t.Fatal(err)
	}
	if err = declarations.Validate(declarations.Model, parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 4 || parsed[0].Offset != 1 || parsed[1].Offset != 7 || parsed[1].Args[1].Value.Text != `a,b;"c"` || parsed[2].Args[0].Value.Args[0].Value.Text != "example.org/events.Opened" || parsed[3].Args[0].Value.Args[1].Value.Text != "2" {
		t.Fatalf("%+v", parsed)
	}
	for _, literal := range []string{`""`, `null`, `true`, `false`, `0`, `-12`, `1.25`, `1e+3`, `1e-3`, `18446744073709551615`} {
		directives, err := declarations.Parse(declarations.V1, "value(E,value="+literal+")")
		if err != nil {
			t.Fatalf("%s: %v", literal, err)
		}
		if err = declarations.Validate(declarations.Model, directives); err != nil {
			t.Fatal(err)
		}
	}
}
func TestMalformedTagsFailWithRedactedTypedErrors(t *testing.T) {
	for _, tag := range []string{`set()`, `set(E,from=a,from=b)`, `set(E,secret="PRIVATE")`, `set(E,from="PRIVATE")`, `value(E,value=PRIVATE)`, `value(E,value="PRIVATE"`, `value(E,value="PRIVATE\q")`, `set(E,from=$eventSourceId)`, `set(id("x",0))`, `set(id("x",1.5))`, `set(go("x",2))`, `set(@)`, `set(E);`, `set(E) set(F)`, `set(E,from=name,E)`, `key(E)`, `pii(value="PRIVATE")`, `children(E,unknown=id)`, `add(E,key=id)`, `encrypted(scope=invalid)`, `set(E,from=x..y)`} {
		t.Run(tag, func(t *testing.T) {
			directives, err := declarations.Parse(declarations.V1, tag)
			if err == nil {
				err = declarations.Validate(declarations.Model, directives)
			}
			var declaration *declarations.DeclarationError
			if !errors.As(err, &declaration) {
				t.Fatalf("expected DeclarationError, got %v", err)
			}
			if strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("literal leaked: %v", err)
			}
			if declaration.Offset < 0 || declaration.Offset > len(tag) {
				t.Fatalf("invalid offset: %+v", declaration)
			}
		})
	}
	if _, err := declarations.Parse(2, "key"); err == nil {
		t.Fatal("unknown version accepted")
	}
	if _, err := declarations.Parse(declarations.V1, strings.Repeat("a", 16385)); err == nil {
		t.Fatal("oversized tag accepted")
	}
}
func TestUnicodeNamesUseByteOffsets(t *testing.T) {
	parsed, err := declarations.Parse(declarations.V1, `set(Åpnet,from=navnØ);context(Åpnet,from=occurred)`)
	if err != nil {
		t.Fatal(err)
	}
	if err = declarations.Validate(declarations.Model, parsed); err != nil {
		t.Fatal(err)
	}
	if parsed[0].Args[0].Value.Text != "Åpnet" || parsed[0].Args[1].Value.Text != "navnØ" || parsed[1].Offset != len("set(Åpnet,from=navnØ);") {
		t.Fatalf("%+v", parsed)
	}
}

func TestTagRolesFailClosed(t *testing.T) {
	for _, tag := range []string{"key", "no-auto", "not-projected", "set(E)"} {
		directives, err := declarations.Parse(declarations.V1, tag)
		if err != nil {
			t.Fatal(err)
		}
		if err = declarations.Validate(declarations.Event, directives); err == nil {
			t.Fatal("event accepted model directive")
		}
	}
}
func FuzzParse(f *testing.F) {
	for _, seed := range []string{`set(E);context(@alias,from=occurred)`, `value(E,value="comma,semi;paren)")`, `set(go("a/b.Event"))`, `set(id("id",2))`, `key`, `value(E,value=null)`, `nested;clear(E)`, `value(E,value=-1.2e+3)`, `set(`, `set(Åpnet,from=navnØ)`, `children(E,key=composite(item=ItemID,source=source),parent-key=context(eventSourceId));remove(R,key=ItemID)`, `all(context=occurred);every(from=name)`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, tag string) {
		first, err := declarations.Parse(declarations.V1, tag)
		second, again := declarations.Parse(declarations.V1, tag)
		if !reflect.DeepEqual(first, second) || (err == nil) != (again == nil) {
			t.Fatal("nondeterministic parse")
		}
		if err != nil {
			var declaration *declarations.DeclarationError
			if !errors.As(err, &declaration) || declaration.Offset < 0 || declaration.Offset > len(tag) {
				t.Fatalf("invalid parser error: %v", err)
			}
			return
		}
		_ = declarations.Validate(declarations.Model, first)
		_ = declarations.Validate(declarations.Event, first)
	})
}
