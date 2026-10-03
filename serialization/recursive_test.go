// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/serialization"
)

type recursiveTree struct {
	Name     string          `json:"name"`
	Children []recursiveTree `json:"children"`
	Next     *recursiveTree  `json:"next"`
}

func TestRecursiveSchemaCodecAndSiblingTraversal(t *testing.T) {
	type Forest struct {
		Left  recursiveTree
		Right recursiveTree
	}
	plan, err := serialization.Compile(reflect.TypeFor[Forest]())
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal([]byte(plan.Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	definitions := schema["definitions"].(map[string]any)
	if len(definitions) != 1 {
		t.Fatalf("definitions: %s", plan.Schema())
	}
	shared := &recursiveTree{Name: "shared"}
	data, err := plan.Marshal(Forest{Left: recursiveTree{Name: "one", Children: []recursiveTree{{Name: "two"}}, Next: shared}, Right: recursiveTree{Next: shared}})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"Left":{"children":[{"name":"two"}],"name":"one","next":{"name":"shared"}},"Right":{"name":"","next":{"name":"shared"}}}` {
		t.Fatalf("payload: %s", data)
	}
	for _, path := range []string{"Left.name", "Right.name"} {
		if _, ok := serialization.FieldAt(plan.Fields(), path); !ok {
			t.Fatalf("sibling %s lost", path)
		}
	}
	left, _ := serialization.FieldAt(plan.Fields(), "Left")
	children, _ := serialization.FieldAt(left.Fields(), "children")
	if _, ok := serialization.FieldAt(children.Fields(), "name"); !ok {
		t.Fatal("recursive local fields unavailable")
	}
	second, err := serialization.Compile(reflect.TypeFor[Forest]())
	if err != nil || second.Schema() != plan.Schema() {
		t.Fatal("unstable recursive schema")
	}
}

func TestRecursiveReadModelKeepsRootIDTranslationLocal(t *testing.T) {
	type Model struct {
		ID       string
		Children []Model
	}
	plan, err := serialization.CompileReadModel(reflect.TypeFor[Model]())
	if err != nil {
		t.Fatal(err)
	}
	data, err := plan.Marshal(Model{ID: "root", Children: []Model{{ID: "child", Children: []Model{{ID: "grandchild"}}}}})
	if err != nil || string(data) != `{"Children":[{"Children":[{"ID":"grandchild"}],"ID":"child"}],"Id":"root"}` {
		t.Fatalf("root naming: %s %v", data, err)
	}
}

func TestRecursiveValuesRejectCyclesAndExcessiveDepth(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[recursiveTree]())
	if err != nil {
		t.Fatal(err)
	}
	cycle := &recursiveTree{}
	cycle.Next = cycle
	if _, err := plan.Marshal(cycle); err == nil || !strings.Contains(err.Error(), "cyclic") {
		t.Fatalf("cycle: %v", err)
	}
	root := &recursiveTree{}
	current := root
	for range 300 {
		current.Next = &recursiveTree{}
		current = current.Next
	}
	if _, err := plan.Marshal(root); err == nil || !strings.Contains(err.Error(), "256") {
		t.Fatalf("depth: %v", err)
	}
}
