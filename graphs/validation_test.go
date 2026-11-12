// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/graphs"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type Keyless struct {
	Name string `json:"name"`
}
type TwoKeys struct {
	First  string `json:"first" chronicle:"key"`
	Second string `json:"second" chronicle:"key"`
}
type Address struct {
	Street string `json:"street"`
}
type WithObject struct {
	ID      string  `json:"id" chronicle:"key"`
	Address Address `json:"address"`
}
type WithObjects struct {
	ID        string    `json:"id" chronicle:"key"`
	Addresses []Address `json:"addresses"`
}
type TwoIdentities struct {
	First  string `json:"first" chronicle:"key"`
	Second string `json:"second" chronicle:"key"`
}
type Note struct {
	Text string `json:"text"`
}

func TestDeclarationsAreRefusedBeforeTheClientConnects(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	person := func() *graphs.Node { return graphs.NewNode[Person]().From(f.registered, graphs.UsingKey("personId")) }
	cases := []struct {
		name  string
		graph func() *graphs.Graph
		want  error
	}{
		{"nameless graph", func() *graphs.Graph { return graphs.New(" ", person()) }, graphs.ErrInvalidGraph},
		{"graph without nodes", func() *graphs.Graph { return graphs.New("G") }, graphs.ErrInvalidGraph},
		{"node without a key", func() *graphs.Graph { return graphs.New("G", graphs.NewNode[Keyless]()) }, graphs.ErrNodeKey},
		{"node with two keys", func() *graphs.Graph { return graphs.New("G", graphs.NewNode[TwoKeys]()) }, graphs.ErrNodeKey},
		{"node with an object property", func() *graphs.Graph { return graphs.New("G", graphs.NewNode[WithObject]()) }, graphs.ErrNestedObject},
		{"node with a list of objects", func() *graphs.Graph { return graphs.New("G", graphs.NewNode[WithObjects]()) }, graphs.ErrNestedObject},
		{"edge with two identifying keys", func() *graphs.Graph {
			p := person()
			return graphs.New("G", p, graphs.NewEdge[TwoIdentities](p, p))
		}, graphs.ErrEdgeIdentity},
		{"edge with an object property", func() *graphs.Graph {
			p := person()
			type Holder struct{ Address Address }
			return graphs.New("G", p, graphs.NewEdge[Holder](p, p))
		}, graphs.ErrNestedObject},
		{"edge without a source", func() *graphs.Graph {
			p := person()
			return graphs.New("G", p, graphs.NewEdge[Note](nil, p))
		}, graphs.ErrEdgeEndpoints},
		{"edge whose target is not part of the graph", func() *graphs.Graph {
			p := person()
			return graphs.New("G", p, graphs.NewEdge[Note](p, graphs.NewNode[Group]()))
		}, graphs.ErrEdgeEndpoints},
		{"two nodes with one label", func() *graphs.Graph { return graphs.New("G", person(), person()) }, graphs.ErrDuplicateElement},
		{"edge type equal to a node label", func() *graphs.Graph {
			p := person()
			return graphs.New("G", p, graphs.NewEdge[Note](p, p).Named("Person"))
		}, graphs.ErrDuplicateElement},
		{"event declared twice on a node", func() *graphs.Graph {
			return graphs.New("G", person().From(f.registered))
		}, graphs.ErrInvalidMapping},
		{"event missing", func() *graphs.Graph { return graphs.New("G", graphs.NewNode[Person]().From(nil)) }, graphs.ErrInvalidGraph},
		{"zero event", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(events.Type[PersonRegistered]{}))
		}, graphs.ErrInvalidGraph},
		{"nil option", func() *graphs.Graph { return graphs.New("G", graphs.NewNode[Person]().From(f.registered, nil)) }, graphs.ErrInvalidGraph},
		{"literal the kernel cannot read", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.SetValue("name", "a,b")))
		}, graphs.ErrInvalidMapping},
		{"unsupported literal type", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.SetValue("name", []string{"x"})))
		}, graphs.ErrInvalidMapping},
		{"property mapped twice", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.Set("name", "name"), graphs.Clear("name")))
		}, graphs.ErrInvalidMapping},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.graph().Build()
			if !errors.Is(err, test.want) {
				t.Fatalf("Build() = %v, want %v", err, test.want)
			}
			if !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("%v is not an invalid configuration", err)
			}
			var declaration *graphs.DeclarationError
			if !errors.As(err, &declaration) || declaration.Graph == "" && test.name != "nameless graph" {
				t.Fatalf("%v does not locate the graph", err)
			}
		})
	}
}

func TestNodeCanBeIdentifiedWithoutATag(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	node := graphs.NewNode[Keyless]().IdentifiedBy("name").From(f.registered)
	definition, err := compile(t, graphs.New("G", node), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	if definition.Schema() == "" {
		t.Fatal("no schema")
	}
	if _, err := compile(t, graphs.New("G", graphs.NewNode[Keyless]().IdentifiedBy("missing").From(f.registered)), f, serialization.PreservePropertyNames); !errors.Is(err, graphs.ErrUnknownProperty) {
		t.Fatalf("an identifying property must exist: %v", err)
	}
}

func TestEdgeCanBeIdentifiedWithoutATag(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	person := graphs.NewNode[Person]().From(f.registered, graphs.UsingKey("personId"))
	edge := graphs.NewEdge[Note](person, person).Named("NOTED").IdentifiedBy("text").
		From(f.addedType, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"), graphs.UsingIdentity("role"))
	definition, err := compile(t, graphs.New("G", person, edge), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	if got := definition.Projection().Graph.Edges["NOTED"].From["member-added+1"]; got.Identity != "role" {
		t.Fatalf("identity = %q", got.Identity)
	}
}

func TestMappingsAreValidatedAgainstTheRegisteredTypes(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	cases := []struct {
		name  string
		graph func() *graphs.Graph
		want  error
	}{
		{"event not registered", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.unregistered))
		}, faults.ErrNotRegistered},
		{"key is not an event property", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.UsingKey("nobody")))
		}, graphs.ErrUnknownProperty},
		{"mapped property is not a node property", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.Set("nickname", "name")))
		}, graphs.ErrUnknownProperty},
		{"mapped from a missing event property", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.Set("name", "nickname")))
		}, graphs.ErrUnknownProperty},
		{"string into a number", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.Set("age", "name")))
		}, graphs.ErrInvalidMapping},
		{"unknown context property", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.SetFromContext("name", "causedBy")))
		}, graphs.ErrUnknownProperty},
		{"event source id into a number", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.SetFromEventSourceID("age")))
		}, graphs.ErrInvalidMapping},
		{"adding to a string", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.Add("name", "age")))
		}, graphs.ErrInvalidMapping},
		{"incrementing a string", func() *graphs.Graph {
			return graphs.New("G", graphs.NewNode[Person]().From(f.registered, graphs.Increment("name")))
		}, graphs.ErrInvalidMapping},
		{"edge identity the event cannot supply", func() *graphs.Graph {
			p := graphs.NewNode[Person]().From(f.registered, graphs.UsingKey("personId"))
			e := graphs.NewEdge[MemberOf](p, p).From(f.removedType, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("personId"))
			return graphs.New("G", p, e)
		}, graphs.ErrEdgeIdentity},
		{"identity without an identifying property", func() *graphs.Graph {
			p := graphs.NewNode[Person]().From(f.registered, graphs.UsingKey("personId"))
			e := graphs.NewEdge[Knows](p, p).From(f.addedType, graphs.UsingIdentity("role"))
			return graphs.New("G", p, e)
		}, graphs.ErrEdgeIdentity},
		{"edge key that is not an event property", func() *graphs.Graph {
			p := graphs.NewNode[Person]().From(f.registered, graphs.UsingKey("personId"))
			e := graphs.NewEdge[Knows](p, p).From(f.addedType, graphs.UsingSourceKey("nobody"))
			return graphs.New("G", p, e)
		}, graphs.ErrUnknownProperty},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			_, err := compile(t, test.graph(), f, serialization.PreservePropertyNames)
			if !errors.Is(err, test.want) {
				t.Fatalf("Compile() = %v, want %v", err, test.want)
			}
		})
	}
}

func TestEventsFromAnotherStoreAreUnsupported(t *testing.T) {
	type External struct {
		PersonID string `json:"personId"`
	}
	external, err := events.Define[External](events.WithID("external"), events.WithSourceStore("other"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(external.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := graphs.New("G", graphs.NewNode[Person]().From(external)).Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := declaration.Compile(catalog, serialization.PreservePropertyNames, graphs.Neo4j); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("Compile() = %v", err)
	}
}
