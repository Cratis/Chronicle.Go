// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/protobuf/proto"
)

type CompositeParts struct {
	Item      string `json:"item"`
	Namespace string `json:"tenant"`
	Constant  string `json:"constant"`
}
type CompositeOrder struct {
	Count int32 `json:"count" chronicle:"count(ItemAdded,key=composite(item=itemId,tenant=context(namespace),constant=value(\"all\"))))"`
}

// Kept separately so the malformed expression above is an admission fixture.
type CompositeTagged struct {
	Count int32 `json:"count" chronicle:"count(ItemAdded,key=composite(item=itemId,tenant=context(namespace),constant=value(\"all\")))"`
}
type CompositeFluent struct {
	Count int32 `json:"count"`
}

func TestCompositeAndContextKeysMatchBothFrontEnds(t *testing.T) {
	event := mustEvent[ItemAdded](t)
	model := mustModel[CompositeTagged](t, readmodels.WithIdentifier("composite"))
	bound := mustCompile(t, projections.ModelBound(model, projections.WithIdentifier("composite")), event.Descriptor()).KernelDefinition()
	var retained *projections.CompositeKeyBuilder[CompositeParts, ItemAdded]
	key := projections.UsingCompositeKey(func(b *projections.CompositeKeyBuilder[CompositeParts, ItemAdded]) {
		retained = b
		projections.KeyPart(b, projections.Path[CompositeParts, string]("item"), projections.Path[ItemAdded, string]("itemId"))
		projections.KeyPartFromContext(b, projections.Path[CompositeParts, string]("tenant"), "namespace")
		projections.KeyPartValue(b, projections.Path[CompositeParts, string]("constant"), "all")
	})
	// Mutation after callback must not alter the captured parts or error state.
	projections.KeyPart(retained, projections.Path[CompositeParts, string]("missing"), projections.Path[ItemAdded, string]("missing"))
	builder := projections.NewBuilder("composite", mustModel[CompositeFluent](t, readmodels.WithIdentifier("composite")))
	projections.From(builder, event, func(f *projections.FromBuilder[CompositeFluent, ItemAdded]) {
		projections.Count(f, projections.Path[CompositeFluent, int32]("count"))
	}, key)
	fluent, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(bound, mustCompile(t, fluent, event.Descriptor()).KernelDefinition()) {
		t.Fatal("composite front ends differ")
	}
	if bound.From[0].Value.Key != "$composite(item=itemId,tenant=$eventContext(namespace),constant=$value(all))" {
		t.Fatal(bound.From[0].Value.Key)
	}
	for _, option := range []projections.FromOption{projections.UsingKeyFromContext("eventSourceId"), projections.UsingParentKeyFromContext("namespace")} {
		d := mustCompile(t, projections.ModelBound(mustModel[CompositeFluent](t), projections.FromEvent(event, option)), event.Descriptor()).KernelDefinition()
		if d.From[0].Value.Key != "$eventContext(eventSourceId)" && d.From[0].Value.ParentKey != "$eventContext(namespace)" {
			t.Fatalf("%+v", d)
		}
	}
}

type ContextKeyTag struct {
	Count int32 `chronicle:"count(ItemAdded,key=context(eventSourceId))"`
}
type ConstantCounter struct {
	Count int32 `chronicle:"increment(ItemAdded,key=value(\"total\"));decrement(ItemRemoved,key=value(\"total\"))"`
}

func TestCounterKeyTags(t *testing.T) {
	event := mustEvent[ItemAdded](t)
	context := mustCompile(t, projections.ModelBound(mustModel[ContextKeyTag](t)), event.Descriptor()).KernelDefinition()
	if context.From[0].Value.Key != "$eventContext(eventSourceId)" {
		t.Fatal(context)
	}
	catalog, err := events.NewCatalog(event.Descriptor(), mustEvent[ItemRemoved](t).Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	d := compileCatalog(t, projections.ModelBound(mustModel[ConstantCounter](t)), catalog).KernelDefinition()
	for _, from := range d.From {
		if from.Value.Key != "$value(total)" {
			t.Fatal(from)
		}
	}
}

type BadArithmetic struct {
	Name string `chronicle:"add(QuantityAdded,from=amount)"`
}
type BadClear struct {
	Name string `chronicle:"clear(QuantityAdded)"`
}
type BadCollectionClear struct {
	Names []string `chronicle:"clear(QuantityAdded)"`
}
type BadNested struct {
	Address OrderAddress `chronicle:"nested;clear(AddressCleared)"`
}
type BadChild struct {
	Item string `chronicle:"children(ItemAdded)"`
}
type BadParentPath struct {
	Items []OrderLine `chronicle:"children(ItemAdded,key=itemId,parent-key=missing)"`
}
type BadIdentityPath struct {
	Items []OrderLine `chronicle:"children(ItemAdded,key=itemId,identified-by=missing)"`
}
type BadJoinPath struct {
	Name string `chronicle:"join(CustomerRenamed,on=missing,from=name)"`
}
type BadRemovePath struct {
	Items []OrderLine `chronicle:"children(ItemAdded,key=itemId);remove(ItemRemoved,key=missing)"`
}
type BadNestedComposite struct {
	Count int32 `chronicle:"count(ItemAdded,key=composite(x=composite(y=itemId,z=orderId)))"`
}
type BadConstant struct {
	Count int32 `chronicle:"count(ItemAdded,key=value(\"PRIVATE,unsupported\"))"`
}
type BadGlobalPath struct {
	Name string `chronicle:"every(from=true);set(CustomerRenamed,from=name)"`
}

func invalidAtClient[T any](t *testing.T) {
	t.Helper()
	registry := chronicle.NewRegistry()
	for _, register := range []func() error{
		func() error { _, e := chronicle.RegisterEvent[QuantityAdded](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[ItemAdded](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[ItemRemoved](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[AddressCleared](registry); return e },
		func() error { _, e := chronicle.RegisterEvent[CustomerRenamed](registry); return e },
	} {
		if err := register(); err != nil {
			t.Fatal(err)
		}
	}
	_, err := chronicle.RegisterReadModel[T](registry)
	if err == nil {
		client, clientErr := chronicle.NewClient(chronicle.WithRegistry(registry))
		if clientErr == nil {
			_ = client.Close()
			t.Fatal("invalid declaration accepted before I/O")
		}
		err = clientErr
	}
	var declaration *projections.DeclarationError
	if !errors.As(err, &declaration) {
		t.Fatalf("missing declaration error: %v", err)
	}
	if strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("literal leaked")
	}
}
func TestAdvancedInvalidDeclarationsFailBeforeIO(t *testing.T) {
	t.Run("arithmetic type", invalidAtClient[BadArithmetic])
	t.Run("nonnullable clear", invalidAtClient[BadClear])
	t.Run("collection clear", invalidAtClient[BadCollectionClear])
	t.Run("nonpointer nested", invalidAtClient[BadNested])
	t.Run("scalar children", invalidAtClient[BadChild])
	t.Run("parent path", invalidAtClient[BadParentPath])
	t.Run("child identity", invalidAtClient[BadIdentityPath])
	t.Run("join path", invalidAtClient[BadJoinPath])
	t.Run("remove path", invalidAtClient[BadRemovePath])
	t.Run("kernel composite parser", invalidAtClient[BadNestedComposite])
	t.Run("kernel literal parser", invalidAtClient[BadConstant])
	t.Run("kernel global path", invalidAtClient[BadGlobalPath])
	t.Run("malformed composite", invalidAtClient[CompositeOrder])
}

func TestAdvancedFluentValidationAndSnapshots(t *testing.T) {
	for name, option := range map[string]projections.FromOption{
		"context": projections.UsingKeyFromContext("unknown"),
		"composite duplicate": projections.UsingCompositeKey(func(b *projections.CompositeKeyBuilder[CompositeParts, ItemAdded]) {
			projections.KeyPart(b, projections.Path[CompositeParts, string]("item"), projections.Path[ItemAdded, string]("itemId"))
			projections.KeyPart(b, projections.Path[CompositeParts, string]("item"), projections.Path[ItemAdded, string]("orderId"))
		}),
		"composite empty": projections.UsingCompositeKey(func(*projections.CompositeKeyBuilder[CompositeParts, ItemAdded]) {}),
	} {
		t.Run(name, func(t *testing.T) {
			b := projections.NewBuilder("invalid", mustModel[CompositeFluent](t))
			projections.From(b, mustEvent[ItemAdded](t), nil, option)
			if _, err := b.Build(); err == nil {
				t.Fatal("invalid key accepted")
			}
		})
	}
	builder := projections.NewBuilder("snapshot", mustModel[FluentOrder](t))
	var retained *projections.Builder[OrderLine]
	projections.Children(builder, projections.Path[FluentOrder, []OrderLine]("items"), func(b *projections.Builder[OrderLine]) {
		retained = b
		projections.From(b, mustEvent[ItemAdded](t), nil)
	})
	retained.Configure(projections.NoAutoMap(), projections.FromEvent(mustEvent[ItemAdded](t)))
	d, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	wire := mustCompile(t, d, mustEvent[ItemAdded](t).Descriptor()).KernelDefinition()
	if len(wire.Children["items"].From) != 1 || wire.Children["items"].AutoMap.String() != "Enabled" {
		t.Fatal("retained builder changed snapshot")
	}
}
