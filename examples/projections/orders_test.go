// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/proto"
)

func TestOrderExamplesPreserveTheirAutoMapDefaults(t *testing.T) {
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
	boundWire, fluentWire := bound.KernelDefinition(), fluent.KernelDefinition()
	if boundWire.Children["lines"].AutoMap != contracts.AutoMap_Enabled || fluentWire.Children["lines"].AutoMap != contracts.AutoMap_Inherit {
		t.Fatal("example front ends lost their AutoMap defaults")
	}
	// Inherit resolves to the Enabled root at runtime. The fluent example
	// explicitly maps identity rather than relying on ChildrenFrom conventions.
	fluentWire.Children["lines"].AutoMap = contracts.AutoMap_Enabled
	if !proto.Equal(boundWire, fluentWire) {
		t.Fatal("example front ends differ beyond their AutoMap defaults")
	}
}
