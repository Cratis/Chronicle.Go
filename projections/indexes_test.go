// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/proto"
)

type IndexOnlyDetail struct {
	City string `chronicle:"index"`
}
type IndexOnlyModel struct {
	ID     string
	Name   string `chronicle:"index"`
	Detail IndexOnlyDetail
}
type UnindexedDetail struct{ City string }
type UnindexedModel struct {
	ID     string
	Name   string
	Detail UnindexedDetail
}

func TestIndexesDoNotBecomeProjectionMappings(t *testing.T) {
	event, err := events.Define[ItemRegistered]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	indexed, err := readmodels.Define[IndexOnlyModel](readmodels.WithIdentifier("same"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := readmodels.Define[UnindexedModel](readmodels.WithIdentifier("same"))
	if err != nil {
		t.Fatal(err)
	}
	if projections.HasMappings(indexed.Descriptor()) {
		t.Fatal("index-only model triggered projection discovery")
	}
	first, err := projections.Compile(projections.ModelBound(indexed, projections.WithIdentifier("projection"), projections.FromEvent(event)), catalog)
	if err != nil {
		t.Fatal(err)
	}
	second, err := projections.Compile(projections.ModelBound(plain, projections.WithIdentifier("projection"), projections.FromEvent(event)), catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(first.KernelDefinition(), second.KernelDefinition()) {
		t.Fatalf("indexes changed projection definition: %v != %v", first.KernelDefinition(), second.KernelDefinition())
	}
}
