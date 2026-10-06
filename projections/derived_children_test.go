// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedchildrenfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/proto"
)

type fluentDerivedLine struct {
	ItemID string `json:"itemId" chronicle:"key"`
	Name   string
}

func (*fluentDerivedLine) Child() {}

type fluentDerivedCatalog struct {
	ID    string `json:"Id" chronicle:"key"`
	Items []derivedchildrenfixtures.Child
}

func derivedEvents(t *testing.T) (events.Type[derivedchildrenfixtures.ItemAdded], events.Type[derivedchildrenfixtures.ItemRemoved], events.Type[derivedchildrenfixtures.ItemRenamed], *events.Catalog) {
	t.Helper()
	added, err := events.Define[derivedchildrenfixtures.ItemAdded]()
	if err != nil {
		t.Fatal(err)
	}
	removed, err := events.Define[derivedchildrenfixtures.ItemRemoved]()
	if err != nil {
		t.Fatal(err)
	}
	renamed, err := events.Define[derivedchildrenfixtures.ItemRenamed]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(added.Descriptor(), removed.Descriptor(), renamed.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return added, removed, renamed, catalog
}

func TestSingleDerivedChildFrontendsAndCapturedCSharpDefinition(t *testing.T) {
	added, removed, renamed, catalog := derivedEvents(t)
	codecs, err := derivedchildrenfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedchildrenfixtures.Catalog](readmodels.WithCodecs(codecs), readmodels.WithIdentifier("Catalog"))
	if err != nil {
		t.Fatal(err)
	}
	bound, err := projections.Compile(projections.ModelBound(model, projections.WithIdentifier("Catalog")), catalog)
	if err != nil {
		t.Fatal(err)
	}
	fluentCodecs, err := serialization.NewCodecs(serialization.Derived[derivedchildrenfixtures.Child, *fluentDerivedLine]("line"))
	if err != nil {
		t.Fatal(err)
	}
	fluentModel, err := readmodels.Define[fluentDerivedCatalog](readmodels.WithCodecs(fluentCodecs), readmodels.WithIdentifier("Catalog"))
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder("Catalog", fluentModel)
	projections.Children(builder, projections.Path[fluentDerivedCatalog, []derivedchildrenfixtures.Child]("Items"), func(child *projections.Builder[fluentDerivedLine]) {
		child.Configure(projections.AutoMap(), projections.RemovedWith(removed, projections.UsingKey(projections.Path[derivedchildrenfixtures.ItemRemoved, string]("ItemId")), projections.UsingParentKey(projections.Path[derivedchildrenfixtures.ItemRemoved, string]("OrderId"))))
		projections.From(child, added, func(from *projections.FromBuilder[fluentDerivedLine, derivedchildrenfixtures.ItemAdded]) {
			projections.Map(from, projections.Path[fluentDerivedLine, string]("itemId"), projections.Path[derivedchildrenfixtures.ItemAdded, string]("ItemId"))
			projections.Map(from, projections.Path[fluentDerivedLine, string]("name"), projections.Path[derivedchildrenfixtures.ItemAdded, string]("Name"))
		}, projections.UsingKey(projections.Path[derivedchildrenfixtures.ItemAdded, string]("ItemId")), projections.UsingParentKey(projections.Path[derivedchildrenfixtures.ItemAdded, string]("OrderId")))
		projections.Join(child, renamed, projections.Path[fluentDerivedLine, string]("itemId"), func(from *projections.FromBuilder[fluentDerivedLine, derivedchildrenfixtures.ItemRenamed]) {
			projections.Map(from, projections.Path[fluentDerivedLine, string]("name"), projections.Path[derivedchildrenfixtures.ItemRenamed, string]("Name"))
		})
	})
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	fluent, err := projections.Compile(declaration, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(bound.KernelDefinition(), fluent.KernelDefinition()) {
		t.Fatalf("frontends differ:\n%s\n%s", bound.KernelDefinition(), fluent.KernelDefinition())
	}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		name := map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "DefaultNamingPolicy", serialization.CamelCase: "CamelCaseNamingPolicy"}[policy]
		t.Run(name, func(t *testing.T) {
			nextModel, err := model.Descriptor().WithNamingPolicy(policy)
			if err != nil {
				t.Fatal(err)
			}
			var descriptors []events.Descriptor
			for _, descriptor := range catalog.Descriptors() {
				next, err := descriptor.WithNamingPolicy(policy)
				if err != nil {
					t.Fatal(err)
				}
				descriptors = append(descriptors, next)
			}
			nextCatalog, err := events.NewCatalog(descriptors...)
			if err != nil {
				t.Fatal(err)
			}
			rebound, err := bound.Rebind(nextModel, catalog, nextCatalog)
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile("testdata/derived-children/" + name + ".definition.json")
			if err != nil {
				t.Fatal(err)
			}
			captured := &contracts.ProjectionDefinition{}
			if err := json.Unmarshal(data, captured); err != nil {
				t.Fatal(err)
			}
			path := "Items"
			if policy == serialization.CamelCase {
				path = "items"
			}
			child := captured.Children[path]
			if child == nil || len(child.From) != 1 || child.From[0].Value.Properties["_derivedTypeId"] != "$value(line)" {
				t.Fatal("capture did not infer a derivative", captured)
			}
			// Explicitly qualified differences, not a fabricated C# capture:
			// C# DefaultNamingPolicy emits ordinary child target casing although
			// its derived serializer emits immediate camelCase. Go uses its plan.
			// Go's explicit JSON event names remain ItemId/OrderId under both policies.
			child.IdentifiedBy = "itemId"
			child.From[0].Value.Key, child.From[0].Value.ParentKey = "ItemId", "OrderId"
			child.RemovedWith[0].Value.Key, child.RemovedWith[0].Value.ParentKey = "ItemId", "OrderId"
			if policy == serialization.PreservePropertyNames {
				delete(child.From[0].Value.Properties, "ItemId")
				delete(child.From[0].Value.Properties, "Name")
				child.From[0].Value.Properties["name"] = "Name"
				delete(child.Join[0].Value.Properties, "Name")
				child.Join[0].Value.Properties["name"] = "Name"
			}
			child.From[0].Value.Properties["itemId"] = "ItemId"
			child.Join[0].Value.On = "itemId"
			if !proto.Equal(rebound.KernelDefinition(), captured) {
				t.Fatalf("qualified capture differs:\ngot %s\nwant %s", rebound.KernelDefinition(), captured)
			}
			first, _ := rebound.Hash()
			copy := rebound.KernelDefinition()
			copy.Children[path].From[0].Value.Properties["_derivedTypeId"] = "$value(other)"
			second, _ := rebound.Hash()
			if first != second {
				t.Fatal("definition snapshot mutable")
			}
		})
	}
}

type otherDerivedLine struct{ Name string }

func (*otherDerivedLine) Child() {}

func TestDerivedChildInferenceRejectsMissingAmbiguousAndUnrepresentableFamilies(t *testing.T) {
	_, _, _, catalog := derivedEvents(t)
	// A family without registered derivatives is refused by the read-model
	// codec before any projection compiles (#64).
	_, err := readmodels.Define[derivedchildrenfixtures.Catalog]()
	if !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("zero variants: %v", err)
	}
	var declaration *projections.DeclarationError
	for _, id := range []string{"ambiguous", "bad)value(x", "line,other", "line\nother", "𐐀", "line\\id"} {
		t.Run(id, func(t *testing.T) {
			registrations := []serialization.Codec{serialization.Derived[derivedchildrenfixtures.Child, *derivedchildrenfixtures.Line](id)}
			if id == "ambiguous" {
				registrations = append(registrations, serialization.Derived[derivedchildrenfixtures.Child, *otherDerivedLine]("other"))
			}
			codecs, err := serialization.NewCodecs(registrations...)
			if err != nil {
				t.Fatal(err)
			}
			model, err := readmodels.Define[derivedchildrenfixtures.Catalog](readmodels.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			_, err = projections.Compile(projections.ModelBound(model), catalog)
			if !errors.As(err, &declaration) || !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("expected declaration error: %v", err)
			}
			if strings.Contains(err.Error(), id) {
				t.Fatal("discriminator leaked")
			}
		})
	}
}

// C# WithInitialValues serializes the whole instance, children included, so the
// initial state carries each derived child with its discriminator.
func TestDerivedChildInitialValuesKeepCollectionsWithDiscriminatorAndRejectTypedNil(t *testing.T) {
	_, _, _, catalog := derivedEvents(t)
	codecs, err := derivedchildrenfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedchildrenfixtures.Catalog](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	initial := derivedchildrenfixtures.Catalog{ID: "order", Items: []derivedchildrenfixtures.Child{&derivedchildrenfixtures.Line{ItemID: "line", Name: "initial"}}}
	compiled, err := projections.Compile(projections.ModelBound(model, projections.WithInitialValues(initial), projections.WithLabels("child", "child")), catalog)
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err := json.Unmarshal([]byte(compiled.KernelDefinition().InitialModelState), &state); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"Id": "order", "Items": []any{map[string]any{"itemId": "line", "name": "initial", "_derivedTypeId": "line"}}}
	if !reflect.DeepEqual(state, want) {
		t.Fatalf("initial state %v, want %v", state, want)
	}
	if !reflect.DeepEqual(compiled.KernelDefinition().Tags, []string{"child"}) {
		t.Fatal("labels changed")
	}
	initial.Items[0] = (*derivedchildrenfixtures.Line)(nil)
	_, err = projections.Compile(projections.ModelBound(model, projections.WithInitialValues(initial)), catalog)
	var declaration *projections.DeclarationError
	if !errors.As(err, &declaration) {
		t.Fatalf("typed nil: %v", err)
	}
}

// The Go read-model codec writes the same derived child payload that the C#
// DerivedTypeJsonConverter wrote in the capture, under both naming policies.
func TestDerivedChildPayloadMatchesCapturedCSharpSerialization(t *testing.T) {
	codecs, err := derivedchildrenfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedchildrenfixtures.Catalog](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	value := derivedchildrenfixtures.Catalog{ID: "order", Items: []derivedchildrenfixtures.Child{&derivedchildrenfixtures.Line{ItemID: "line", Name: "Ada"}}}
	for name, policy := range map[string]serialization.NamingPolicy{"DefaultNamingPolicy": serialization.PreservePropertyNames, "CamelCaseNamingPolicy": serialization.CamelCase} {
		t.Run(name, func(t *testing.T) {
			descriptor, err := model.Descriptor().WithNamingPolicy(policy)
			if err != nil {
				t.Fatal(err)
			}
			data, err := descriptor.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			captured, err := os.ReadFile("testdata/derived-children/" + name + ".child.json")
			if err != nil {
				t.Fatal(err)
			}
			var got, want any
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(captured, &want); err != nil {
				t.Fatal(err)
			}
			if policy == serialization.CamelCase {
				// Qualified difference: the explicit json:"Id" tag pins the Go
				// root key under every policy; C# camel-cases its Id property.
				root := want.(map[string]any)
				root["Id"] = root["id"]
				delete(root, "id")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("payload %s, captured %s", data, captured)
			}
			decoded, err := descriptor.Unmarshal(captured)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded, &value) {
				t.Fatalf("decoded %#v", decoded)
			}
		})
	}
}

func TestDerivedChildrenCaptureProvenance(t *testing.T) {
	data, err := os.ReadFile("testdata/derived-children/provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var provenance struct {
		Fixtures []string          `json:"fixtures"`
		SHA256   map[string]string `json:"sha256"`
	}
	if err := json.Unmarshal(data, &provenance); err != nil {
		t.Fatal(err)
	}
	if len(provenance.Fixtures) != 4 || len(provenance.SHA256) != 9 {
		t.Fatalf("provenance lists %d fixtures and %d hashes", len(provenance.Fixtures), len(provenance.SHA256))
	}
	for name, want := range provenance.SHA256 {
		content, err := os.ReadFile("testdata/derived-children/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(content)); got != want {
			t.Errorf("%s sha256 %s, provenance %s", name, got, want)
		}
	}
}
