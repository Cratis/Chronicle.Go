// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

func TestCapturedPreparationPreservesCodecProtectionAndOriginalModelHandle(t *testing.T) {
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	classifications := 0
	classifier := compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		classifications++
		return compliance.Classification{}, nil
	})
	registry := NewRegistry()
	event, err := RegisterEvent[factoryCodecCurrent](registry, events.WithCodecs(codecs), events.WithProtection(classifier))
	if err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[factoryCodecModel](registry, readmodels.WithCodecs(codecs), readmodels.WithProtection(classifier))
	if err != nil {
		t.Fatal(err)
	}
	classified := classifications
	member := &derivedfixtures.HumanValue{Name: "frozen", Children: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 7}}}
	labels := []string{"captured"}
	if err := registry.AddProjection(projections.ModelBound(model, projections.FromEvent(event), projections.WithLabels(labels...), projections.WithInitialValues(factoryCodecModel{DisplayName: "initial", Member: member}))); err != nil {
		t.Fatal(err)
	}
	p := captureForTest(t, WithRegistry(registry), WithRegistryForStore("same", registry), WithNamingPolicy(serialization.CamelCase))
	*codecs = serialization.Codecs{}
	member.Name, labels[0] = "changed", "changed"
	declareEvent[catalogReplacement](t, registry)
	if classifications != classified {
		t.Fatal("capture ran a classification provider")
	}
	client, err := p.Prepare(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, store := range []StoreName{"default", "same"} {
		artifacts, err := client.Artifacts(store)
		if err != nil {
			t.Fatal(err)
		}
		if len(artifacts.Events.Descriptors()) != 1 {
			t.Fatal("late event leaked")
		}
		projection := artifacts.Projections[0]
		definition := projection.KernelDefinition()
		value, err := projection.Model().Unmarshal([]byte(definition.InitialModelState))
		if err != nil {
			t.Fatal(err)
		}
		want := &derivedfixtures.HumanValue{Name: "frozen", Children: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 7}}}
		if !reflect.DeepEqual(value.(*factoryCodecModel).Member, want) || definition.Tags[0] != "captured" {
			t.Fatalf("initial state or labels changed: member=%#v want=%#v labels=%v", value.(*factoryCodecModel).Member, want, definition.Tags)
		}
		service, err := readmodels.New(store, DefaultNamespace, artifacts.ReadModels, &clientTransport{})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := readmodels.For(service, model).Get(ctx, "key"); !errors.Is(err, context.Canceled) {
			t.Fatal("original model handle lost", err)
		}
	}
	if classifications == 0 || classifications != classified {
		t.Fatal("protection classification reran")
	}
}
