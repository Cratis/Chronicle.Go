// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type BoundTally struct {
	ID        uuid.UUID   `chronicle:"key"`
	Title     string      `chronicle:"set(TallyRecorded,from=Label,once=true)"`
	CreatedAt time.Time   `chronicle:"context(TallyRecorded,from=occurred,once=true)"`
	Status    string      `chronicle:"value(TallyRecorded,value=\"new\",once=true)"`
	Label     string      `chronicle:"set(TallyRecorded,once=false)"`
	Recent    []TallyLine `chronicle:"children(TallyLineAdded,key=LineID,prepend=true)"`
	Ranked    []TallyLine `chronicle:"children(TallyLineAdded,key=LineID,order-by-descending=Position)"`
	Named     []TallyLine `chronicle:"children(TallyLineAdded,key=LineID,order-by=Name)"`
	Lines     []TallyLine `chronicle:"children(TallyLineAdded,key=LineID)"`
}

func compileBound[M any](t *testing.T) (projections.Definition, error) {
	t.Helper()
	f := newTallyFixture(t)
	return projections.Compile(projections.ModelBound(mustModel[M](t, readmodels.WithIdentifier("BoundTally"))), f.catalog)
}

func TestModelBoundSetOnceAndChildrenOrdering(t *testing.T) {
	definition, err := compileBound[BoundTally](t)
	if err != nil {
		t.Fatal(err)
	}
	wire := definition.KernelDefinition()
	expectProperties(t, fromProperties(t, wire, "tally-recorded"), map[string]string{
		"Title":     "$setOnce(Label)",
		"CreatedAt": "$setOnce($eventContext(occurred))",
		"Status":    "$setOnce($value(new))",
		"Label":     "Label",
	})
	if got := wire.Children["Recent"].GetOrdering(); got.GetPlacement() != contracts.ChildrenPlacement_Prepend {
		t.Fatalf("prepend: %v", got)
	}
	if got := wire.Children["Ranked"].GetOrdering(); got.GetPlacement() != contracts.ChildrenPlacement_Sorted || got.GetSortBy() != "Position" || got.GetDirection() != contracts.SortDirection_Descending {
		t.Fatalf("descending: %v", got)
	}
	if got := wire.Children["Named"].GetOrdering(); got.GetPlacement() != contracts.ChildrenPlacement_Sorted || got.GetSortBy() != "Name" || got.GetDirection() != contracts.SortDirection_Ascending {
		t.Fatalf("ascending: %v", got)
	}
	if got := wire.Children["Lines"].GetOrdering(); got != nil {
		t.Fatalf("unordered: %v", got)
	}
}

type boundTwoOrderings struct {
	ID    uuid.UUID   `chronicle:"key"`
	Lines []TallyLine `chronicle:"children(TallyLineAdded,key=LineID,prepend=true,order-by=Name)"`
}
type boundDisagreeingOrderings struct {
	ID    uuid.UUID   `chronicle:"key"`
	Lines []TallyLine `chronicle:"children(TallyLineAdded,key=LineID,order-by=Name);children(TallyRecorded,key=Label,prepend=true)"`
}
type boundUnknownSort struct {
	ID    uuid.UUID   `chronicle:"key"`
	Lines []TallyLine `chronicle:"children(TallyLineAdded,key=LineID,order-by=Rank)"`
}
type boundOnceNull struct {
	ID    uuid.UUID `chronicle:"key"`
	Title *string   `chronicle:"value(TallyRecorded,value=null,once=true)"`
}

func TestModelBoundOrderingAndOnceRejectInvalidDeclarations(t *testing.T) {
	for name, compile := range map[string]func(*testing.T) (projections.Definition, error){
		"two orderings":         compileBound[boundTwoOrderings],
		"disagreeing orderings": compileBound[boundDisagreeingOrderings],
		"unknown sort property": compileBound[boundUnknownSort],
		"null once":             compileBound[boundOnceNull],
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := compile(t); !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("expected invalid configuration, got %v", err)
			}
		})
	}
}

type boundOnceNotBoolean struct {
	ID    uuid.UUID `chronicle:"key"`
	Title string    `chronicle:"set(TallyRecorded,once=yes)"`
}

func TestModelBoundOnceRequiresBoolean(t *testing.T) {
	model, err := readmodels.Define[boundOnceNotBoolean]()
	if err == nil {
		_, err = projections.Compile(projections.ModelBound(model), newTallyFixture(t).catalog)
	}
	if err == nil {
		t.Fatal("once=yes must be rejected")
	}
}
