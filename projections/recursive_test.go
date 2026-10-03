// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/proto"
)

type ModuleCreated struct{ Name string }
type FeatureAdded struct{ ModuleID, FeatureID, Name string }
type SubFeatureAdded struct{ ParentID, Name string }
type FeatureUpdated struct{ FeatureID, Name string }
type NestedFeatureUpdated struct{ FeatureID, ParentID, Name string }
type RecursiveFeature struct {
	ID          string `json:"id" chronicle:"key"`
	Name        string
	Local       string             `chronicle:"no-auto"`
	SubFeatures []RecursiveFeature `chronicle:"children(SubFeatureAdded,identified-by=id,parent-key=ParentID)"`
}
type RecursiveModule struct {
	ID       string `json:"id" chronicle:"key"`
	Name     string
	Features []RecursiveFeature `chronicle:"children(FeatureAdded,key=FeatureID,identified-by=id,parent-key=ModuleID)"`
	Other    []RecursiveFeature `chronicle:"children(FeatureAdded,key=FeatureID,identified-by=id,parent-key=ModuleID)"`
}

// Hand-derived from C# with_children_having/self_referencing_children* specs at
// 2e31b0d: finite definitions include one repeated node, without ancestor creators.
func TestRecursiveKeyedChildPropagationAndAncestorExclusion(t *testing.T) {
	created := mustEvent[ModuleCreated](t)
	added := mustEvent[FeatureAdded](t)
	sub := mustEvent[SubFeatureAdded](t)
	updated := mustEvent[FeatureUpdated](t)
	nestedUpdate := mustEvent[NestedFeatureUpdated](t)
	catalog, _ := events.NewCatalog(created.Descriptor(), added.Descriptor(), sub.Descriptor(), updated.Descriptor(), nestedUpdate.Descriptor())
	declaration := projections.ModelBound(mustModel[RecursiveModule](t), projections.FromEvent(created), projections.WithNodes(projections.Node[RecursiveFeature](
		projections.FromEvent(added),
		projections.FromEvent(sub, projections.UsingParentKey(projections.Path[SubFeatureAdded, string]("ParentID"))),
		projections.FromEvent(updated, projections.UsingKey(projections.Path[FeatureUpdated, string]("FeatureID"))),
		projections.FromEvent(nestedUpdate, projections.UsingKey(projections.Path[NestedFeatureUpdated, string]("FeatureID")), projections.UsingParentKey(projections.Path[NestedFeatureUpdated, string]("ParentID"))),
	)))
	definition, err := projections.Compile(declaration, catalog)
	if err != nil {
		t.Fatal(err)
	}
	d := definition.KernelDefinition()
	fluent := fluentRecursiveDefinition(t, created, added, sub, updated, nestedUpdate, catalog)
	if !proto.Equal(d, fluent.KernelDefinition()) {
		t.Fatalf("fluent recursion differs: %v", fluent.KernelDefinition())
	}
	first := d.Children["Features"]
	other := d.Children["Other"]
	if first == nil || other == nil || !proto.Equal(first, other) {
		t.Fatal("global visited set dropped sibling")
	}
	if hasChildEvent(first, "SubFeatureAdded") || !hasChildEvent(first, "FeatureUpdated") {
		t.Fatalf("root child subscriptions: %v", first.From)
	}
	nested := first.Children["SubFeatures"]
	if nested == nil || len(nested.Children) != 0 || hasChildEvent(nested, "FeatureAdded") || hasChildEvent(nested, "FeatureUpdated") || !hasChildEvent(nested, "SubFeatureAdded") || !hasChildEvent(nested, "NestedFeatureUpdated") {
		t.Fatalf("recursive subscriptions: %v", nested)
	}
	if nested.IdentifiedBy != "id" || len(nested.NoAutoMapProperties) != 1 || nested.NoAutoMapProperties[0] != "Local" {
		t.Fatal("recursive identity/exclusions lost")
	}
	for _, from := range nested.From {
		if from.Key.Id == "SubFeatureAdded" && (from.Value.ParentKey != "ParentID" || from.Value.Properties["id"] != "$eventContext(EventSourceId)") {
			t.Fatal("recursive creator identity/key")
		}
	}
	model, err := definition.Model().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	var eventsAfter []events.Descriptor
	for _, event := range catalog.Descriptors() {
		rebound, err := event.WithNamingPolicy(serialization.CamelCase)
		if err != nil {
			t.Fatal(err)
		}
		eventsAfter = append(eventsAfter, rebound)
	}
	after, _ := events.NewCatalog(eventsAfter...)
	rebound, err := definition.Rebind(model, catalog, after)
	if err != nil {
		t.Fatal(err)
	}
	if rebound.KernelDefinition().Children["features"].Children["subFeatures"].NoAutoMapProperties[0] != "local" {
		t.Fatal("recursive naming not rebound")
	}
	h1, err := definition.Hash()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := rebound.Hash()
	if err != nil || h1 == h2 {
		t.Fatal("hash did not include nested naming")
	}
}

type FluentRecursiveFeature struct {
	ID          string `json:"id" chronicle:"key"`
	Name        string
	Local       string `chronicle:"no-auto"`
	SubFeatures []FluentRecursiveFeature
}
type FluentRecursiveModule struct {
	ID       string `json:"id" chronicle:"key"`
	Name     string
	Features []FluentRecursiveFeature
	Other    []FluentRecursiveFeature
}

func fluentRecursiveDefinition(t *testing.T, created events.Type[ModuleCreated], added events.Type[FeatureAdded], sub events.Type[SubFeatureAdded], updated events.Type[FeatureUpdated], nestedUpdate events.Type[NestedFeatureUpdated], catalog *events.Catalog) projections.Definition {
	t.Helper()
	id := "github.com/cratis/chronicle.go/projections_test.RecursiveModule"
	builder := projections.NewBuilder(id, mustModel[FluentRecursiveModule](t, readmodels.WithIdentifier(readmodels.Identifier(id))))
	projections.From(builder, created, nil)
	define := func(child *projections.Builder[FluentRecursiveFeature]) {
		child.Configure(projections.AutoMap())
		projections.From(child, added, func(from *projections.FromBuilder[FluentRecursiveFeature, FeatureAdded]) {
			projections.Map(from, projections.Path[FluentRecursiveFeature, string]("id"), projections.Path[FeatureAdded, string]("FeatureID"))
		}, projections.UsingKey(projections.Path[FeatureAdded, string]("FeatureID")), projections.UsingParentKey(projections.Path[FeatureAdded, string]("ModuleID")))
		projections.From(child, updated, nil, projections.UsingKey(projections.Path[FeatureUpdated, string]("FeatureID")))
		projections.From(child, nestedUpdate, nil, projections.UsingKey(projections.Path[NestedFeatureUpdated, string]("FeatureID")), projections.UsingParentKey(projections.Path[NestedFeatureUpdated, string]("ParentID")))
		projections.Children(child, projections.Path[FluentRecursiveFeature, []FluentRecursiveFeature]("SubFeatures"), func(nested *projections.Builder[FluentRecursiveFeature]) {
			nested.Configure(projections.AutoMap())
			projections.From(nested, sub, func(from *projections.FromBuilder[FluentRecursiveFeature, SubFeatureAdded]) {
				projections.Context(from, projections.Path[FluentRecursiveFeature, string]("id"), "EventSourceId")
			}, projections.UsingParentKey(projections.Path[SubFeatureAdded, string]("ParentID")))
			projections.From(nested, nestedUpdate, nil, projections.UsingKey(projections.Path[NestedFeatureUpdated, string]("FeatureID")), projections.UsingParentKey(projections.Path[NestedFeatureUpdated, string]("ParentID")))
		})
	}
	projections.Children(builder, projections.Path[FluentRecursiveModule, []FluentRecursiveFeature]("Features"), define)
	projections.Children(builder, projections.Path[FluentRecursiveModule, []FluentRecursiveFeature]("Other"), define)
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	definition, err := projections.Compile(declaration, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func hasChildEvent(child *contracts.ChildrenDefinition, id string) bool {
	for _, from := range child.From {
		if from.Key.Id == id {
			return true
		}
	}
	return false
}
