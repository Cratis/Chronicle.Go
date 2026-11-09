// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/graphs"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type PersonRegistered struct {
	PersonID string `json:"personId"`
	Name     string `json:"name"`
	Age      int    `json:"age"`
}
type PersonRemoved struct {
	PersonID string `json:"personId"`
}
type GroupCreated struct {
	GroupID string `json:"groupId"`
	Title   string `json:"title"`
}
type MemberAdded struct {
	PersonID string `json:"personId"`
	GroupID  string `json:"groupId"`
	Role     string `json:"role"`
}
type MemberRemoved struct {
	PersonID string `json:"personId"`
	GroupID  string `json:"groupId"`
	Role     string `json:"role"`
}
type Unregistered struct {
	PersonID string `json:"personId"`
}

type Person struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}
type Group struct {
	ID          string   `json:"id" chronicle:"key"`
	Title       string   `json:"title"`
	MemberCount int      `json:"memberCount"`
	Labels      []string `json:"labels"`
}
type MemberOf struct {
	Role  string    `json:"role" chronicle:"key"`
	Since time.Time `json:"since"`
}
type Knows struct {
	Since time.Time `json:"since"`
}

type fixture struct {
	catalog      *events.Catalog
	registered   events.Type[PersonRegistered]
	removedType  events.Type[PersonRemoved]
	createdType  events.Type[GroupCreated]
	addedType    events.Type[MemberAdded]
	leftType     events.Type[MemberRemoved]
	unregistered events.Type[Unregistered]
}

func newFixture(t *testing.T, policy serialization.NamingPolicy) *fixture {
	t.Helper()
	f := &fixture{}
	var descriptors []events.Descriptor
	define := func(descriptor events.Descriptor, err error) events.Descriptor {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		bound, err := descriptor.WithNamingPolicy(policy)
		if err != nil {
			t.Fatal(err)
		}
		descriptors = append(descriptors, bound)
		return descriptor
	}
	var err error
	f.registered, err = events.Define[PersonRegistered](events.WithID("person-registered"))
	define(f.registered.Descriptor(), err)
	f.removedType, err = events.Define[PersonRemoved](events.WithID("person-removed"))
	define(f.removedType.Descriptor(), err)
	f.createdType, err = events.Define[GroupCreated](events.WithID("group-created"))
	define(f.createdType.Descriptor(), err)
	f.addedType, err = events.Define[MemberAdded](events.WithID("member-added"))
	define(f.addedType.Descriptor(), err)
	f.leftType, err = events.Define[MemberRemoved](events.WithID("member-removed"))
	define(f.leftType.Descriptor(), err)
	f.unregistered, err = events.Define[Unregistered](events.WithID("unregistered"))
	if err != nil {
		t.Fatal(err)
	}
	f.catalog, err = events.NewCatalog(descriptors...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// organization declares the same graph the other clients use in their documentation.
func organization(f *fixture) *graphs.Graph {
	person := graphs.NewNode[Person]().
		From(f.registered, graphs.UsingKey("personId")).
		RemovedWith(f.removedType, graphs.UsingKey("personId"), graphs.Detach())
	group := graphs.NewNode[Group]().
		From(f.createdType, graphs.UsingKey("groupId")).
		From(f.addedType, graphs.UsingKey("groupId"), graphs.Increment("memberCount")).
		From(f.leftType, graphs.UsingKey("groupId"), graphs.Decrement("memberCount"))
	memberOf := graphs.NewEdge[MemberOf](person, group).
		Named("MEMBER_OF").
		From(f.addedType, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"), graphs.SetFromContext("since", "occurred")).
		RemovedWith(f.leftType, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"))
	return graphs.New("Organization", person, group, memberOf)
}

func compile(t *testing.T, graph *graphs.Graph, f *fixture, policy serialization.NamingPolicy) (graphs.Definition, error) {
	t.Helper()
	declaration, err := graph.Build()
	if err != nil {
		return graphs.Definition{}, err
	}
	return declaration.Compile(f.catalog, policy, graphs.Neo4j)
}

func TestOrganizationGraphWireDefinition(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	definition, err := compile(t, organization(f), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	projection := definition.Projection()
	if projection.Identifier != "Organization" || projection.ReadModel != "Organization" || projection.EventSequenceId != "event-log" || !projection.IsActive || !projection.IsRewindable || projection.InitialModelState != "{}" {
		t.Fatalf("projection header = %v", projection)
	}
	person := projection.Graph.Nodes["Person"]
	if got := person.From["person-registered+1"]; got == nil || got.Key != "personId" || got.UpdateOnly || got.Properties["name"] != "name" || got.Properties["age"] != "age" || got.Properties["id"] != "" {
		t.Fatalf("person from = %v", got)
	}
	if got := person.RemovedWith["person-removed+1"]; got == nil || got.Key != "personId" || !got.Detach {
		t.Fatalf("person removed with = %v", got)
	}
	group := projection.Graph.Nodes["Group"]
	if got := group.From["member-added+1"]; got == nil || got.Key != "groupId" || got.Properties["memberCount"] != "$increment" {
		t.Fatalf("group added = %v", got)
	}
	if got := group.From["member-removed+1"]; got.Properties["memberCount"] != "$decrement" {
		t.Fatalf("group removed = %v", got)
	}
	if got := group.From["group-created+1"]; got.Properties["title"] != "title" || len(got.Properties) != 1 {
		t.Fatalf("group created = %v", got)
	}
	edge := projection.Graph.Edges["MEMBER_OF"]
	added := edge.From["member-added+1"]
	if added == nil || added.SourceKey != "personId" || added.TargetKey != "groupId" || added.Identity != "role" || added.Properties["since"] != "$eventContext(occurred)" || len(added.Properties) != 1 {
		t.Fatalf("edge from = %v", added)
	}
	left := edge.RemovedWith["member-removed+1"]
	if left == nil || left.SourceKey != "personId" || left.TargetKey != "groupId" || left.Identity != "role" {
		t.Fatalf("edge removed = %v", left)
	}
	readModel := definition.ReadModel()
	if readModel.Type.Identifier != "Organization" || readModel.Type.Generation != 1 || readModel.Sink.TypeId != "Neo4j" || readModel.ObserverIdentifier != "Organization" || readModel.Schema != definition.Schema() {
		t.Fatalf("read model = %v", readModel)
	}
}

func TestGraphSchemaAnnotations(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	definition, err := compile(t, organization(f), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := json.Unmarshal([]byte(definition.Schema()), &schema); err != nil {
		t.Fatal(err)
	}
	if schema["x-graph"] != true || schema["title"] != "Organization" || schema["type"] != "object" {
		t.Fatalf("root = %v", schema)
	}
	elements := schema["properties"].(map[string]any)
	person := elements["Person"].(map[string]any)
	if person["x-graph-kind"] != "Node" || person["x-graph-key"] != "id" {
		t.Fatalf("person = %v", person)
	}
	if _, ok := person["properties"].(map[string]any)["name"]; !ok {
		t.Fatalf("person properties = %v", person["properties"])
	}
	edge := elements["MEMBER_OF"].(map[string]any)
	if edge["x-graph-kind"] != "Edge" || edge["x-graph-source"] != "Person" || edge["x-graph-target"] != "Group" || edge["x-graph-identity"] != "role" {
		t.Fatalf("edge = %v", edge)
	}
	labels := elements["Group"].(map[string]any)["properties"].(map[string]any)["labels"].(map[string]any)
	if labels["type"] != "array" {
		t.Fatalf("a list of primitives is a graph property: %v", labels)
	}
}

func TestEdgeWithoutIdentifyingPropertyHasNoIdentity(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	person := graphs.NewNode[Person]().From(f.registered, graphs.UsingKey("personId"))
	knows := graphs.NewEdge[Knows](person, person).From(f.addedType, graphs.UsingSourceKey("personId"), graphs.UsingTargetKey("groupId"), graphs.SetFromContext("since", "occurred"))
	definition, err := compile(t, graphs.New("Social", person, knows), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	if got := definition.Projection().Graph.Edges["Knows"].From["member-added+1"]; got.Identity != "" || got.SourceKey != "personId" {
		t.Fatalf("edge = %v", got)
	}
	var schema map[string]map[string]map[string]any
	_ = json.Unmarshal([]byte(definition.Schema()), &schema)
	if _, present := schema["properties"]["Knows"]["x-graph-identity"]; present {
		t.Fatal("an edge without an identifying property has no x-graph-identity")
	}
}

func TestDefaultsToEventSourceIDAndMapsSameNamedProperties(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	group := graphs.NewNode[Group]().From(f.createdType).RemovedWith(f.createdType)
	definition, err := compile(t, graphs.New("Groups", group), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	node := definition.Projection().Graph.Nodes["Group"]
	if node.From["group-created+1"].Key != "$eventSourceId" || node.RemovedWith["group-created+1"].Key != "$eventSourceId" {
		t.Fatalf("keys = %v %v", node.From, node.RemovedWith)
	}
}

func TestMappingsEncodeLikeTheOtherClients(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	type Counter struct {
		ID      string  `json:"id" chronicle:"key"`
		Total   int     `json:"total"`
		Score   float64 `json:"score"`
		Label   string  `json:"label"`
		Active  bool    `json:"active"`
		Seen    string  `json:"seen"`
		Cleared string  `json:"cleared"`
		At      string  `json:"at"`
		Source  string  `json:"source"`
	}
	counting := graphs.NewNode[Counter]().From(f.registered, graphs.Count("seen"))
	if _, err := compile(t, graphs.New("Counters", counting), f, serialization.PreservePropertyNames); !errors.Is(err, graphs.ErrInvalidMapping) {
		t.Fatalf("a string property cannot be counted: %v", err)
	}
	node := graphs.NewNode[Counter]().From(f.registered,
		graphs.Add("total", "age"),
		graphs.Subtract("score", "age"),
		graphs.SetValue("label", "Gold Tier"),
		graphs.SetValue("active", true),
		graphs.Clear("cleared"),
		graphs.SetFromContext("at", "occurred"),
		graphs.SetFromEventSourceID("source"),
	)
	definition, err := compile(t, graphs.New("Counters", node), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	got := definition.Projection().Graph.Nodes["Counter"].From["person-registered+1"].Properties
	want := map[string]string{
		"total": "$add(age)", "score": "$subtract(age)", "label": "$value(Gold Tier)", "active": "$value(True)",
		"cleared": "$null", "at": "$eventContext(occurred)", "source": "$eventSourceId",
	}
	for property, expression := range want {
		if got[property] != expression {
			t.Errorf("%s = %q, want %q (all: %v)", property, got[property], expression, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("properties = %v", got)
	}
}

func TestNamingPolicyDecidesPropertyNames(t *testing.T) {
	type Plain struct {
		Identifier string `chronicle:"key"`
		FullName   string
	}
	type Renamed struct {
		PersonID string
		FullName string
	}
	_ = Renamed{}
	f := newFixture(t, serialization.CamelCase)
	node := graphs.NewNode[Plain]().From(f.registered, graphs.UsingKey("personId"), graphs.Set("fullName", "name"))
	definition, err := compile(t, graphs.New("Plain", node), f, serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	from := definition.Projection().Graph.Nodes["Plain"].From["person-registered+1"]
	if from.Properties["fullName"] != "name" {
		t.Fatalf("properties = %v", from.Properties)
	}
	if _, err := compile(t, graphs.New("Plain", graphs.NewNode[Plain]().From(f.registered, graphs.Set("FullName", "name"))), f, serialization.CamelCase); !errors.Is(err, graphs.ErrUnknownProperty) {
		t.Fatalf("the serialized name is the camel case one: %v", err)
	}
	var schema map[string]any
	_ = json.Unmarshal([]byte(definition.Schema()), &schema)
	if schema["properties"].(map[string]any)["Plain"].(map[string]any)["x-graph-key"] != "identifier" {
		t.Fatalf("schema = %v", schema)
	}
}

func TestDefaultAndExplicitSink(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	node := func() *graphs.Node { return graphs.NewNode[Person]().From(f.registered) }
	definition, err := compile(t, graphs.New("A", node()), f, serialization.PreservePropertyNames)
	if err != nil || definition.Sink() != graphs.Neo4j || definition.ReadModel().Sink.TypeId != "Neo4j" {
		t.Fatalf("default sink = %v %v", definition.Sink(), err)
	}
	definition, err = compile(t, graphs.New("A", node()).UsingSink(graphs.InMemoryGraph), f, serialization.PreservePropertyNames)
	if err != nil || definition.ReadModel().Sink.TypeId != "InMemoryGraph" {
		t.Fatalf("explicit sink = %v", err)
	}
	if _, err := graphs.New("A", node()).UsingSink("MongoDB").Build(); !errors.Is(err, graphs.ErrInvalidGraph) {
		t.Fatalf("a graph needs a graph sink: %v", err)
	}
}

func TestDefinitionsAreDetached(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	definition, err := compile(t, organization(f), f, serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	first := definition.Projection()
	first.Identifier = "changed"
	delete(first.Graph.Nodes, "Person")
	if second := definition.Projection(); second.Identifier != "Organization" || second.Graph.Nodes["Person"] == nil {
		t.Fatal("a copy changed the definition")
	}
	if !proto.Equal(definition.Projection(), definition.Projection()) {
		t.Fatal("definitions differ between calls")
	}
	if len(definition.Nodes()) != 2 || len(definition.Edges()) != 1 || len(definition.Events()) != 5 {
		t.Fatalf("summary = %v %v %v", definition.Nodes(), definition.Edges(), definition.Events())
	}
}

func TestBuilderMutationAfterBuildDoesNotChangeTheDeclaration(t *testing.T) {
	f := newFixture(t, serialization.PreservePropertyNames)
	person := graphs.NewNode[Person]().From(f.registered, graphs.UsingKey("personId"))
	declaration, err := graphs.New("G", person).Build()
	if err != nil {
		t.Fatal(err)
	}
	person.From(f.removedType).Labeled("Changed")
	definition, err := declaration.Compile(f.catalog, serialization.PreservePropertyNames, graphs.Neo4j)
	if err != nil {
		t.Fatal(err)
	}
	if len(definition.Nodes()) != 1 || definition.Nodes()[0] != "Person" || len(definition.Events()) != 1 {
		t.Fatalf("definition = %v %v", definition.Nodes(), definition.Events())
	}
}

func TestDefinitionPropertiesDoNotShareState(t *testing.T) {
	var zero graphs.Definition
	if zero.Name() != "" || zero.Projection() != nil || zero.ReadModel() != nil || zero.Nodes() != nil || zero.Events() != nil || zero.Schema() != "" {
		t.Fatal("the zero value is invalid and empty")
	}
	var declaration graphs.Declaration
	if _, err := declaration.Compile(nil, serialization.PreservePropertyNames, graphs.Neo4j); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("zero declaration: %v", err)
	}
}
