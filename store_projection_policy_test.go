// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type historyMixedAllModel struct {
	ID      string
	Name    string    `chronicle:"set(historyAdmissionEvent)"`
	Updated time.Time `chronicle:"all(context=occurred)"`
}

// Chronicle#4562 is fixed in 19.32.1: replay and history fold every event the
// projection handles live, so mixed ALL plus explicit mappings is admitted.
func TestMixedAllHistoryIsAdmitted(t *testing.T) {
	r := NewRegistry()
	if _, err := RegisterEvent[historyAdmissionEvent](r); err != nil {
		t.Fatal(err)
	}
	m, err := RegisterReadModel[historyMixedAllModel](r)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(r))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	snapshot, err := client.selectedStoreSnapshot("store")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.projections) != 1 || !snapshot.projections[0].KernelDefinition().SubscribesToAllEvents || len(snapshot.projections[0].KernelDefinition().From) == 0 {
		t.Fatal("fixture is not a mixed ALL projection")
	}
	validator, err := projectionReplayValidatorFor(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	d, _ := snapshot.models.LookupIdentifier(m.Identifier())
	if known, err := validator(t.Context(), d); !known || err != nil {
		t.Fatal("mixed ALL replay refused", known, err)
	}
}

func TestProjectionReplayPolicyOwnsBoundReplacementAndInboxDefinitions(t *testing.T) {
	build := func(source string, defaults bool) (*Registry, readmodels.Model[historyAdmissionModel]) {
		r := NewRegistry()
		options := []events.TypeOption{}
		if source != "" {
			options = append(options, events.WithSourceStore(source))
		}
		e, err := RegisterEvent[historyAdmissionEvent](r, options...)
		if err != nil {
			t.Fatal(err)
		}
		m, err := RegisterReadModel[historyAdmissionModel](r)
		if err != nil {
			t.Fatal(err)
		}
		p := []projections.Option{projections.FromEvent(e)}
		if defaults {
			p = append(p, projections.WithInitialValues(historyAdmissionModel{Name: "default"}))
		}
		if err := r.AddProjection(projections.ModelBound(m, p...)); err != nil {
			t.Fatal(err)
		}
		return r, m
	}
	fallback, _ := build("", true)
	replacement, m := build("origin", false)
	client, err := NewClient(WithRegistry(fallback), WithRegistryForStore("target", replacement))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	snapshot, err := client.selectedStoreSnapshot("target")
	if err != nil {
		t.Fatal(err)
	}
	validator, err := projectionReplayValidatorFor(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	d, _ := snapshot.models.LookupIdentifier(m.Identifier())
	if d.EventSequence() != events.SequenceID(events.InboxPrefix+"origin") {
		t.Fatal("policy not store bound")
	}
	// Mutating detached protobuf copies and the input slice cannot change policy.
	wire := snapshot.projections[0].KernelDefinition()
	wire.InitialModelState = `{"Name":"changed"}`
	wire.SubscribesToAllEvents = true
	snapshot.projections[0] = projections.Definition{}
	if known, err := validator(t.Context(), d); !known || err != nil {
		t.Fatal("policy borrowed input", err)
	}
	other, err := client.selectedStoreSnapshot("other")
	if err != nil {
		t.Fatal(err)
	}
	fallbackValidator, err := projectionReplayValidatorFor(other)
	if err != nil {
		t.Fatal(err)
	}
	otherModel, _ := other.models.LookupIdentifier(m.Identifier())
	if known, err := fallbackValidator(t.Context(), otherModel); !known || !errors.Is(err, ErrUnsupported) {
		t.Fatal("replacement leaked to default policy", err)
	}
}
