// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"os"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestFluentChildWithoutParentKeyMatchesCSharpGolden(t *testing.T) {
	// Hand-derived from C# ChildrenBuilder, ProjectionBuilder.Children and
	// KeyAndParentKeyBuilder at 2e31b0dfba489159b3db323238f16d0f277056b4,
	// not captured from a .NET run. NoExpression serializes as an empty string;
	// the kernel, not the fluent builder, defaults it to $eventSourceId.
	event := mustEvent[ItemAdded](t, events.WithID("item-added"))
	builder := projections.NewBuilder("fluent-child", mustModel[FluentOrder](t, readmodels.WithIdentifier("Order")))
	projections.Children(builder, projections.Path[FluentOrder, []OrderLine]("items"), func(child *projections.Builder[OrderLine]) {
		projections.From(child, event, nil, projections.UsingKey(projections.Path[ItemAdded, string]("itemId")))
	}, projections.IdentifiedBy(projections.Path[OrderLine, string]("id")))
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("testdata/fluent-child.json")
	if err != nil {
		t.Fatal(err)
	}
	expected := &contracts.ProjectionDefinition{}
	if err = protojson.Unmarshal(data, expected); err != nil {
		t.Fatal(err)
	}
	definition := mustCompile(t, declaration, event.Descriptor())
	actual := definition.KernelDefinition()
	if !proto.Equal(actual, expected) {
		t.Fatalf("got %s\nwant %s", protojson.Format(actual), protojson.Format(expected))
	}
	if len(definition.Diagnostics()) != 0 {
		t.Fatalf("fluent parent inference ran: %+v", definition.Diagnostics())
	}
}

type CompositeChildOwner struct {
	Items []OrderLine `json:"items" chronicle:"children(ItemAdded,key=composite(item=itemId,tenant=orderId),parent-key=orderId)"`
}

func TestCompositeChildKeysDoNotWriteScalarIdentity(t *testing.T) {
	event := mustEvent[ItemAdded](t)
	bound := projections.ModelBound(mustModel[CompositeChildOwner](t))
	builder := projections.NewBuilder("composite-child", mustModel[FluentOrder](t))
	projections.Children(builder, projections.Path[FluentOrder, []OrderLine]("items"), func(child *projections.Builder[OrderLine]) {
		projections.From(child, event, nil, projections.UsingCompositeKey(func(key *projections.CompositeKeyBuilder[CompositeParts, ItemAdded]) {
			projections.KeyPart(key, projections.Path[CompositeParts, string]("item"), projections.Path[ItemAdded, string]("itemId"))
			projections.KeyPart(key, projections.Path[CompositeParts, string]("tenant"), projections.Path[ItemAdded, string]("orderId"))
		}), projections.UsingParentKey(projections.Path[ItemAdded, string]("orderId")))
	}, projections.IdentifiedBy(projections.Path[OrderLine, string]("id")))
	fluent, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	for name, declaration := range map[string]projections.Declaration{"model-bound": bound, "fluent": fluent} {
		t.Run(name, func(t *testing.T) {
			child := mustCompile(t, declaration, event.Descriptor()).KernelDefinition().Children["items"]
			from := child.From[0].Value
			if child.IdentifiedBy != "id" || from.Key != "$composite(item=itemId,tenant=orderId)" || from.ParentKey != "orderId" || len(from.Properties) != 0 {
				t.Fatalf("composite child: %s", protojson.Format(child))
			}
		})
	}
}

func TestModelBoundAutoMapChecksOnlyImmediateParent(t *testing.T) {
	catalog, err := events.NewCatalog(mustEvent[ItemAdded](t).Descriptor(), mustEvent[SubItemAdded](t).Descriptor(), mustEvent[SubItemRemoved](t).Descriptor(), mustEvent[CustomerRemoved](t).Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for name, options := range map[string][]projections.Option{
		"root disabled":  {projections.NoAutoMap()},
		"child disabled": {projections.WithNodes(projections.Node[ParentChild](projections.NoAutoMap()))},
	} {
		t.Run(name, func(t *testing.T) {
			wire := compileCatalog(t, projections.ModelBound(mustModel[Grandparent](t), options...), catalog).KernelDefinition()
			child := wire.Children["Items"]
			grandchild := child.Children["SubItems"]
			want := contracts.AutoMap_Enabled
			if name == "child disabled" {
				want = contracts.AutoMap_Disabled
			}
			if child.AutoMap != contracts.AutoMap_Disabled || grandchild.AutoMap != want {
				t.Fatalf("child AutoMap = %s, grandchild = %s, want Disabled/%s", child.AutoMap, grandchild.AutoMap, want)
			}
		})
	}
}

func TestRootRemovedWithJoinReportsKernelLimitation(t *testing.T) {
	event := mustEvent[CustomerRemoved](t)
	definition := mustCompile(t, projections.ModelBound(mustModel[CompositeFluent](t), projections.RemovedWithJoin(event)), event.Descriptor())
	diagnostics := definition.Diagnostics()
	if len(diagnostics) != 1 || diagnostics[0].Message != "kernel ignores root RemovedWithJoin: root removal via join is not supported (Chronicle#4263)" || diagnostics[0].Replacement.Event != event.Descriptor().Ref() {
		t.Fatalf("diagnostics = %+v", diagnostics)
	}
	if len(definition.KernelDefinition().RemovedWithJoin) != 1 {
		t.Fatal("diagnostic erased the removal definition")
	}
}

func TestFluentChildAutoMapDefaultAndOverrides(t *testing.T) {
	for name, options := range map[string][]projections.Option{
		"inherited": nil,
		"enabled":   {projections.AutoMap()},
		"disabled":  {projections.NoAutoMap()},
	} {
		t.Run(name, func(t *testing.T) {
			builder := projections.NewBuilder("auto", mustModel[FluentOrder](t), projections.NoAutoMap())
			projections.Children(builder, projections.Path[FluentOrder, []OrderLine]("items"), func(child *projections.Builder[OrderLine]) {
				child.Configure(options...)
				projections.From(child, mustEvent[ItemAdded](t), nil)
			})
			declaration, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			wire := mustCompile(t, declaration, mustEvent[ItemAdded](t).Descriptor()).KernelDefinition()
			want := map[string]contracts.AutoMap{"inherited": contracts.AutoMap_Inherit, "enabled": contracts.AutoMap_Enabled, "disabled": contracts.AutoMap_Disabled}[name]
			if wire.AutoMap != contracts.AutoMap_Disabled || wire.Children["items"].AutoMap != want {
				t.Fatalf("AutoMap: %s", protojson.Format(wire))
			}
		})
	}
}
