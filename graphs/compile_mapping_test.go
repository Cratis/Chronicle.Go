// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/graphs"
	"github.com/cratis/chronicle.go/serialization"
)

func TestMappingValidationPreservesListAndSourceCheckOrder(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	for _, test := range []struct {
		name   string
		option graphs.NodeFromOption
		want   error
	}{
		{"list before missing event property", graphs.Set("labels", "missing"), graphs.ErrInvalidMapping},
		{"missing context property before list", graphs.SetFromContext("labels", "missing"), graphs.ErrUnknownProperty},
		{"missing arithmetic property before target type", graphs.Add("title", "missing"), graphs.ErrUnknownProperty},
		{"list cannot take constant", graphs.SetValue("labels", "value"), graphs.ErrInvalidMapping},
		{"list cannot take source ID", graphs.SetFromEventSourceID("labels"), graphs.ErrInvalidMapping},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := graphs.NewNode[Group]().From(f.createdType, test.option)
			_, err := compile(t, graphs.New("G", node), f, serialization.PreservePropertyNames)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestAutomaticMappingsIncludePrimitiveListsButExcludeKeysAndIncompatibleTypes(t *testing.T) {
	definition := compileAutomaticMappingGraph(t)
	node := definition.Projection().Graph.Nodes["Group"]
	if node == nil {
		t.Fatal("missing Group node")
	}
	from := node.From["group-mapped+1"]
	if from == nil {
		t.Fatal("missing group-mapped event")
	}
	if !reflect.DeepEqual(from.Properties, map[string]string{"labels": "labels"}) {
		t.Fatalf("properties = %v", from.Properties)
	}
}

func compileAutomaticMappingGraph(t *testing.T) graphs.Definition {
	t.Helper()
	type registered struct {
		ID     string   `json:"id"`
		Title  int      `json:"title"`
		Labels []string `json:"labels"`
	}
	event, err := events.Define[registered](events.WithID("group-mapped"))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := event.Descriptor().WithNamingPolicy(serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := graphs.New("G", graphs.NewNode[Group]().From(event)).Build()
	if err != nil {
		t.Fatal(err)
	}
	definition, err := declaration.Compile(catalog, serialization.PreservePropertyNames, graphs.Neo4j)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}
