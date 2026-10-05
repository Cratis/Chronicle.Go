// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type DerivedCopied struct{ Members []derivedfixtures.Member }
type derivedProjectionModel struct {
	ID      string
	Members []derivedfixtures.Member `chronicle:"set(DerivedCopied)"`
}
type derivedChildrenModel struct {
	ID      string
	Members []derivedfixtures.Member `chronicle:"children(DerivedCopied)"`
}

func TestDerivedProjectionCopiesWholeFamiliesButDoesNotInferChildren(t *testing.T) {
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[DerivedCopied](events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedProjectionModel](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	initial := derivedProjectionModel{Members: derivedfixtures.Sample().Members}
	declaration := projections.ModelBound(model, projections.WithInitialValues(initial))
	initial.Members[0] = derivedfixtures.RobotValue{Count: 999}
	compiled, err := projections.Compile(declaration, catalog)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.KernelDefinition().From[0].Value.Properties["Members"] != "Members" {
		t.Fatal("whole family copy missing")
	}
	if state := compiled.KernelDefinition().InitialModelState; !strings.Contains(state, `"_derivedTypeId":"human"`) || strings.Contains(state, "999") {
		t.Fatalf("initial snapshot changed: %s", state)
	}
	// Initial-value snapshot renaming follows compiled variants, not arbitrary callbacks.
	clientModel, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	data, err := model.Descriptor().Marshal(derivedProjectionModel{Members: derivedfixtures.Sample().Members})
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := clientModel.RebindJSON(data, model.Descriptor())
	if err != nil || !strings.Contains(string(rebound), `"displayName":"nested"`) {
		t.Fatalf("snapshot: %s %v", rebound, err)
	}
	children, err := readmodels.Define[derivedChildrenModel](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projections.Compile(projections.ModelBound(children), catalog); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("premature child inference: %v", err)
	}
	changed, err := serialization.NewCodecs(serialization.Derived[derivedfixtures.Member, *derivedfixtures.HumanValue]("human"), serialization.Derived[derivedfixtures.Member, derivedfixtures.RobotValue]("different"))
	if err != nil {
		t.Fatal(err)
	}
	incompatible, err := readmodels.Define[derivedProjectionModel](readmodels.WithCodecs(changed))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := projections.Compile(projections.ModelBound(incompatible), catalog); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("incompatible discriminator copy: %v", err)
	}
}
