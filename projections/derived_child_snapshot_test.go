// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedchildrenfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/proto"
)

type derivedSnapshotRoot struct {
	Name  string `chronicle:"index"`
	Items []derivedchildrenfixtures.Child
	Plain *fluentDerivedLine
}

func TestDerivedChildSnapshotsRebindWithoutCallbacksOrRootIndexCollisions(t *testing.T) {
	added := mustEvent[derivedchildrenfixtures.ItemAdded](t, events.WithSourceStore("origin"))
	codecs, err := serialization.NewCodecs(serialization.Derived[derivedchildrenfixtures.Child, *fluentDerivedLine]("line"))
	if err != nil {
		t.Fatal(err)
	}
	var metadataCalls, builderCalls int
	model, err := readmodels.Define[derivedSnapshotRoot](readmodels.WithCodecs(codecs), readmodels.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		metadataCalls++
		return compliance.Classification{}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	frozenCalls := metadataCalls
	initial := derivedSnapshotRoot{Name: "root", Items: []derivedchildrenfixtures.Child{&fluentDerivedLine{ItemID: "discarded"}}, Plain: &fluentDerivedLine{ItemID: "ordinary", Name: "plain"}}
	builder := projections.NewBuilder("snapshot", model, projections.WithLabels("one", "two", "one"), projections.WithInitialValues(initial))
	projections.Nested(builder, projections.Path[derivedSnapshotRoot, *fluentDerivedLine]("Plain"), nil)
	projections.Children(builder, projections.Path[derivedSnapshotRoot, []derivedchildrenfixtures.Child]("Items"), func(child *projections.Builder[fluentDerivedLine]) {
		builderCalls++
		projections.From(child, added, func(from *projections.FromBuilder[fluentDerivedLine, derivedchildrenfixtures.ItemAdded]) {
			projections.Map(from, projections.Path[fluentDerivedLine, string]("name"), projections.Path[derivedchildrenfixtures.ItemAdded, string]("Name"))
		})
	})
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	initial.Plain.Name = "changed"
	before, _ := events.NewCatalog(added.Descriptor())
	compiled, err := projections.Compile(declaration, before)
	if err != nil {
		t.Fatal(err)
	}
	nextModel, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	nextEvent, err := added.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := events.NewCatalog(nextEvent)
	rebound, err := compiled.Rebind(nextModel, before, after)
	if err != nil {
		t.Fatal(err)
	}
	one, err := rebound.ForStore("target")
	if err != nil {
		t.Fatal(err)
	}
	two, err := rebound.ForStore("target")
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(one.KernelDefinition(), two.KernelDefinition()) {
		t.Fatal("store binding drift")
	}
	if metadataCalls != frozenCalls || builderCalls != 1 {
		t.Fatalf("callbacks reran: metadata %d/%d, builder %d", metadataCalls, frozenCalls, builderCalls)
	}
	if !reflect.DeepEqual(one.Model().Indexes(), []string{"name"}) {
		t.Fatal("root index collided with derivative", one.Model().Indexes())
	}
	// Initial values are a snapshot taken at declaration time (the later change
	// to initial.Plain is not observed); children keep their discriminator.
	if got := one.KernelDefinition().InitialModelState; got != `{"items":[{"_derivedTypeId":"line","itemId":"discarded","name":""}],"name":"root","plain":{"itemId":"ordinary","name":"plain"}}` {
		t.Fatal("initial snapshot changed", got)
	}
	if one.EventSequence() != "inbox-origin" {
		t.Fatal("child source subscription lost")
	}
	local, err := rebound.ForStore("origin")
	if err != nil || local.EventSequence() != events.EventLog {
		t.Fatal("local child source changed", err)
	}
}
