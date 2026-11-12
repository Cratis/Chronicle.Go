// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
)

type TallyRecorded struct {
	Category string
	Amount   int64
	Label    string
}
type TallyDropped struct {
	Category string
}
type TallyLine struct {
	ID       string
	Position int32
	Name     string
}
type TallyLineAdded struct {
	LineID   string
	Position int32
	Name     string
}
type Tally struct {
	ID         uuid.UUID `chronicle:"key"`
	Totals     map[string]int64
	Counts     map[string]int64
	ByUser     map[string]int64
	FirstLabel map[string]string
	Title      string
	CreatedAt  time.Time
	CreatedBy  string
	LastEditor string
	Status     string
	Source     string
	Lines      []TallyLine
	Recent     []TallyLine
	Ranked     []TallyLine
}

type tallyFixture struct {
	recorded events.Type[TallyRecorded]
	dropped  events.Type[TallyDropped]
	added    events.Type[TallyLineAdded]
	model    readmodels.Model[Tally]
	catalog  *events.Catalog
}

func newTallyFixture(t *testing.T) tallyFixture {
	t.Helper()
	f := tallyFixture{
		recorded: mustEvent[TallyRecorded](t, events.WithID("tally-recorded")),
		dropped:  mustEvent[TallyDropped](t, events.WithID("tally-dropped")),
		added:    mustEvent[TallyLineAdded](t, events.WithID("tally-line-added")),
		model:    mustModel[Tally](t, readmodels.WithIdentifier("Tally")),
	}
	catalog, err := events.NewCatalog(f.recorded.Descriptor(), f.dropped.Descriptor(), f.added.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	f.catalog = catalog
	return f
}

func (f tallyFixture) compile(t *testing.T, b *projections.Builder[Tally]) (projections.Definition, error) {
	t.Helper()
	declaration, err := b.Build()
	if err != nil {
		return projections.Definition{}, err
	}
	return projections.Compile(declaration, f.catalog)
}

func (f tallyFixture) mustCompile(t *testing.T, b *projections.Builder[Tally]) projections.Definition {
	t.Helper()
	definition, err := f.compile(t, b)
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

var (
	tallyTotals     = projections.Path[Tally, map[string]int64]("Totals")
	tallyCounts     = projections.Path[Tally, map[string]int64]("Counts")
	tallyByUser     = projections.Path[Tally, map[string]int64]("ByUser")
	tallyFirstLabel = projections.Path[Tally, map[string]string]("FirstLabel")
	tallyCategory   = projections.Path[TallyRecorded, string]("Category")
	droppedCategory = projections.Path[TallyDropped, string]("Category")
)

func expectProperties(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("property %q: got %q, want %q (all: %v)", key, got[key], value, got)
		}
	}
}

func fromProperties(t *testing.T, definition *contracts.ProjectionDefinition, event string) map[string]string {
	t.Helper()
	for _, from := range definition.From {
		if from.Key.Id == event {
			return from.Value.Properties
		}
	}
	t.Fatalf("no From for %s in %s", event, protojson.Format(definition))
	return nil
}

func TestDictionaryEntriesEmitDynamicKeyPaths(t *testing.T) {
	f := newTallyFixture(t)
	b := projections.NewBuilder("tally", f.model)
	projections.From(b, f.recorded, func(from *projections.FromBuilder[Tally, TallyRecorded]) {
		projections.Add(from, projections.Entry(tallyTotals, projections.KeyFromEvent(tallyCategory)), projections.Path[TallyRecorded, int64]("Amount"))
		projections.Increment(from, projections.Entry(tallyCounts, projections.KeyFromContext("eventSourceId")))
		projections.Count(from, projections.Entry(tallyByUser, projections.KeyFromCausedBy(projections.CausedByUserName)))
		projections.MapOnce(from, projections.Entry(tallyFirstLabel, projections.KeyFromEvent(tallyCategory)), projections.Path[TallyRecorded, string]("Label"))
	})
	projections.From(b, f.dropped, func(from *projections.FromBuilder[Tally, TallyDropped]) {
		projections.RemoveKey(from, projections.Entry(tallyTotals, projections.KeyFromEvent(droppedCategory)))
	})
	wire := f.mustCompile(t, b).KernelDefinition()
	expectProperties(t, fromProperties(t, wire, "tally-recorded"), map[string]string{
		"Totals.$eventContent.Category":      "$add(Amount)",
		"Counts.$eventContext.eventSourceId": "$increment",
		"ByUser.$causedBy.userName":          "$count",
		"FirstLabel.$eventContent.Category":  "$setOnce(Label)",
	})
	expectProperties(t, fromProperties(t, wire, "tally-dropped"), map[string]string{
		"Totals.$eventContent.Category": "$remove",
	})
}

func TestDictionaryEntriesRejectInvalidTargetsAndKeys(t *testing.T) {
	f := newTallyFixture(t)
	for name, define := range map[string]func(*projections.FromBuilder[Tally, TallyRecorded]){
		"key missing on event": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.Increment(from, projections.Entry(tallyCounts, projections.KeyFromEvent(projections.Path[TallyRecorded, string]("category"))))
		},
		"unknown context key": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.Increment(from, projections.Entry(tallyCounts, projections.KeyFromContext("missing")))
		},
		"invalid caused-by key": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.Increment(from, projections.Entry(tallyCounts, projections.KeyFromCausedBy("email")))
		},
		"zero key": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.Increment(from, projections.Entry(tallyCounts, projections.EntryKey{}))
		},
		"not a dictionary": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.Map(from, projections.Path[Tally, string]("Title.$eventContent.Category"), projections.Path[TallyRecorded, string]("Label"))
		},
		"value type mismatch": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.Map(from, projections.Entry(projections.Path[Tally, map[string]int64]("FirstLabel"), projections.KeyFromEvent(tallyCategory)), projections.Path[TallyRecorded, int64]("Amount"))
		},
		"remove without entry": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.RemoveKey(from, projections.Path[Tally, string]("Title"))
		},
		"duplicate entry": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.Increment(from, projections.Entry(tallyCounts, projections.KeyFromEvent(tallyCategory)))
			projections.Decrement(from, projections.Entry(tallyCounts, projections.KeyFromEvent(tallyCategory)))
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := projections.NewBuilder("tally", f.model)
			projections.From(b, f.recorded, define)
			if _, err := f.compile(t, b); !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("expected invalid configuration, got %v", err)
			}
		})
	}
}

func TestDictionaryEntriesWithDifferentKeySourcesCoexist(t *testing.T) {
	f := newTallyFixture(t)
	b := projections.NewBuilder("tally", f.model)
	projections.From(b, f.recorded, func(from *projections.FromBuilder[Tally, TallyRecorded]) {
		projections.Increment(from, projections.Entry(tallyCounts, projections.KeyFromEvent(tallyCategory)))
		projections.Increment(from, projections.Entry(tallyCounts, projections.KeyFromContext("eventSourceId")))
	})
	properties := fromProperties(t, f.mustCompile(t, b).KernelDefinition(), "tally-recorded")
	expectProperties(t, properties, map[string]string{
		"Counts.$eventContent.Category":      "$increment",
		"Counts.$eventContext.eventSourceId": "$increment",
	})
}

func TestSetOnceWrapsEveryEventValueProvider(t *testing.T) {
	f := newTallyFixture(t)
	b := projections.NewBuilder("tally", f.model)
	projections.From(b, f.recorded, func(from *projections.FromBuilder[Tally, TallyRecorded]) {
		projections.MapOnce(from, projections.Path[Tally, string]("Title"), projections.Path[TallyRecorded, string]("Label"))
		projections.ContextOnce(from, projections.Path[Tally, time.Time]("CreatedAt"), "occurred")
		projections.CausedByOnce(from, projections.Path[Tally, string]("CreatedBy"), projections.CausedBySubject)
		projections.CausedBy(from, projections.Path[Tally, string]("LastEditor"), projections.CausedByName)
		projections.ValueOnce(from, projections.Path[Tally, string]("Status"), "new")
		projections.EventSourceIDOnce(from, projections.Path[Tally, string]("Source"))
	})
	expectProperties(t, fromProperties(t, f.mustCompile(t, b).KernelDefinition(), "tally-recorded"), map[string]string{
		"Title":      "$setOnce(Label)",
		"CreatedAt":  "$setOnce($eventContext(occurred))",
		"CreatedBy":  "$setOnce($causedBy(subject))",
		"LastEditor": "$causedBy(name)",
		"Status":     "$setOnce($value(new))",
		"Source":     "$setOnce($eventSourceId)",
	})
}

type nullableTally struct {
	ID    uuid.UUID `chronicle:"key"`
	Title *string
}

func TestSetOnceRejectsNullAndCausedByRejectsNonStringTargets(t *testing.T) {
	f := newTallyFixture(t)
	nullable := projections.NewBuilder("tally", mustModel[nullableTally](t))
	projections.From(nullable, f.recorded, func(from *projections.FromBuilder[nullableTally, TallyRecorded]) {
		projections.ValueOnce(from, projections.Path[nullableTally, *string]("Title"), nil)
	})
	if _, err := nullable.Build(); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("set once null: expected invalid configuration, got %v", err)
	}
	for name, define := range map[string]func(*projections.FromBuilder[Tally, TallyRecorded]){
		"caused-by time": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.CausedBy(from, projections.Path[Tally, time.Time]("CreatedAt"), projections.CausedByName)
		},
		"caused-by unknown property": func(from *projections.FromBuilder[Tally, TallyRecorded]) {
			projections.CausedBy(from, projections.Path[Tally, string]("CreatedBy"), "email")
		},
	} {
		t.Run(name, func(t *testing.T) {
			b := projections.NewBuilder("tally", f.model)
			projections.From(b, f.recorded, define)
			if _, err := f.compile(t, b); !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("expected invalid configuration, got %v", err)
			}
		})
	}
}

func TestEveryAndAllSupportSetOnceCausedByAndContextKeyedEntries(t *testing.T) {
	f := newTallyFixture(t)
	b := projections.NewBuilder("tally", f.model)
	projections.From(b, f.recorded, nil)
	projections.Every(b, func(every *projections.EveryBuilder[Tally]) {
		projections.EveryContextOnce(every, projections.Path[Tally, time.Time]("CreatedAt"), "occurred")
		projections.EveryCausedBy(every, projections.Path[Tally, string]("LastEditor"), projections.CausedByUserName)
		projections.EveryMapOnce(every, projections.Path[Tally, string]("Title"), "Label")
		projections.EveryContext(every, projections.Entry(projections.Path[Tally, map[string]string]("FirstLabel"), projections.KeyFromCausedBy(projections.CausedBySubject)), "eventSourceId")
	})
	wire := f.mustCompile(t, b).KernelDefinition()
	expectProperties(t, wire.All.Properties, map[string]string{
		"CreatedAt":                    "$setOnce($eventContext(occurred))",
		"LastEditor":                   "$causedBy(userName)",
		"Title":                        "$setOnce(Label)",
		"FirstLabel.$causedBy.subject": "$eventContext(eventSourceId)",
	})

	content := projections.NewBuilder("tally", f.model)
	projections.From(content, f.recorded, nil)
	projections.Every(content, func(every *projections.EveryBuilder[Tally]) {
		projections.EveryContext(every, projections.Entry(projections.Path[Tally, map[string]string]("FirstLabel"), projections.KeyFromEvent(tallyCategory)), "eventSourceId")
	})
	if _, err := f.compile(t, content); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("event-content key in Every: expected invalid configuration, got %v", err)
	}
}

func tallyChildren(f tallyFixture, b *projections.Builder[Tally], path string, options ...projections.ChildOption) {
	projections.Children(b, projections.Path[Tally, []TallyLine](path), func(child *projections.Builder[TallyLine]) {
		projections.From(child, f.added, nil, projections.UsingKey(projections.Path[TallyLineAdded, string]("LineID")))
	}, options...)
}

func TestChildrenOrderingEncodesPlacementAndSort(t *testing.T) {
	f := newTallyFixture(t)
	b := projections.NewBuilder("tally", f.model)
	tallyChildren(f, b, "Lines")
	tallyChildren(f, b, "Recent", projections.Prepend())
	tallyChildren(f, b, "Ranked", projections.OrderByDescending(projections.Path[TallyLine, int32]("Position")))
	wire := f.mustCompile(t, b).KernelDefinition()
	if ordering := wire.Children["Lines"].GetOrdering(); ordering != nil {
		t.Fatalf("unordered children must not emit ordering: %v", ordering)
	}
	if got := wire.Children["Recent"].GetOrdering(); got.GetPlacement() != contracts.ChildrenPlacement_Prepend || got.GetSortBy() != "" {
		t.Fatalf("prepend: %v", got)
	}
	got := wire.Children["Ranked"].GetOrdering()
	if got.GetPlacement() != contracts.ChildrenPlacement_Sorted || got.GetSortBy() != "Position" || got.GetDirection() != contracts.SortDirection_Descending {
		t.Fatalf("sorted: %v", got)
	}

	ascending := projections.NewBuilder("tally", f.model)
	tallyChildren(f, ascending, "Lines", projections.OrderBy(projections.Path[TallyLine, string]("Name")))
	got = f.mustCompile(t, ascending).KernelDefinition().Children["Lines"].GetOrdering()
	if got.GetPlacement() != contracts.ChildrenPlacement_Sorted || got.GetSortBy() != "Name" || got.GetDirection() != contracts.SortDirection_Ascending {
		t.Fatalf("ascending: %v", got)
	}
}

func TestChildrenOrderingRejectsInvalidDeclarations(t *testing.T) {
	f := newTallyFixture(t)
	for name, options := range map[string][]projections.ChildOption{
		"twice":       {projections.Prepend(), projections.OrderBy(projections.Path[TallyLine, int32]("Position"))},
		"unknown":     {projections.OrderBy(projections.Path[TallyLine, int32]("Rank"))},
		"wrong type":  {projections.OrderBy(projections.Path[TallyLine, string]("Position"))},
		"wrong owner": {projections.OrderBy(projections.Path[Tally, string]("Title"))},
	} {
		t.Run(name, func(t *testing.T) {
			b := projections.NewBuilder("tally", f.model)
			tallyChildren(f, b, "Lines", options...)
			if _, err := f.compile(t, b); !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("expected invalid configuration, got %v", err)
			}
		})
	}
}

func TestRebindRenamesDictionaryKeysAndSortProperties(t *testing.T) {
	f := newTallyFixture(t)
	b := projections.NewBuilder("tally", f.model)
	projections.From(b, f.recorded, func(from *projections.FromBuilder[Tally, TallyRecorded]) {
		projections.Add(from, projections.Entry(tallyTotals, projections.KeyFromEvent(tallyCategory)), projections.Path[TallyRecorded, int64]("Amount"))
	})
	tallyChildren(f, b, "Ranked", projections.OrderBy(projections.Path[TallyLine, int32]("Position")))
	definition := f.mustCompile(t, b)
	model, err := f.model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	var descriptors []events.Descriptor
	for _, descriptor := range f.catalog.Descriptors() {
		camel, err := descriptor.WithNamingPolicy(serialization.CamelCase)
		if err != nil {
			t.Fatal(err)
		}
		descriptors = append(descriptors, camel)
	}
	after, err := events.NewCatalog(descriptors...)
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := definition.Rebind(model, f.catalog, after)
	if err != nil {
		t.Fatal(err)
	}
	wire := rebound.KernelDefinition()
	expectProperties(t, fromProperties(t, wire, "tally-recorded"), map[string]string{"totals.$eventContent.category": "$add(amount)"})
	if got := wire.Children["ranked"].GetOrdering().GetSortBy(); got != "position" {
		t.Fatalf("sort property not rebound: %q", got)
	}
}
