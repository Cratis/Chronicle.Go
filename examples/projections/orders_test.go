// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/proto"
)

func TestOrderExamplesHaveIdenticalDefinitions(t *testing.T) {
	placed, err := events.Define[OrderPlaced]()
	if err != nil {
		t.Fatal(err)
	}
	added, err := events.Define[LineAdded]()
	if err != nil {
		t.Fatal(err)
	}
	renamed, err := events.Define[CustomerRenamed]()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[Order](readmodels.WithIdentifier("example-order"))
	if err != nil {
		t.Fatal(err)
	}
	fluentModel, err := readmodels.Define[FluentOrder](readmodels.WithIdentifier("example-order"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(placed.Descriptor(), added.Descriptor(), renamed.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	bound, err := projections.Compile(modelBoundOrder(model, placed), catalog)
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := fluentOrder(fluentModel, placed, added, renamed)
	if err != nil {
		t.Fatal(err)
	}
	fluent, err := projections.Compile(declaration, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(bound.KernelDefinition(), fluent.KernelDefinition()) {
		t.Fatal("example front ends differ")
	}
}
