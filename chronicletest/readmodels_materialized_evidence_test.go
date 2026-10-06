// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
)

func TestMaterializedScenarioRetriesObserverEvidenceNotSinkContent(t *testing.T) {
	f, k := materializedFixture(t)
	if err := f.scenario.Given(t.Context(), "source", subscriptionAdded{}); err != nil {
		t.Fatal(err)
	}
	k.missingOnce = true
	got, err := f.scenario.Instance(t.Context())
	if err != nil || !got.Exists || got.Value.Name != "from real sink" {
		t.Fatalf("retry: %+v %v", got, err)
	}
	if !reflect.DeepEqual(k.calls, []string{"wait", "observer", "wait", "observer", "failures", "sink"}) {
		t.Fatalf("evidence ordering: %v", k.calls)
	}
}
func TestMaterializedScenarioRefusesObserverBeyondTail(t *testing.T) {
	f, k := materializedFixture(t)
	if err := f.scenario.Given(t.Context(), "source", subscriptionAdded{}); err != nil {
		t.Fatal(err)
	}
	k.position = 1
	got, err := f.scenario.InstanceFor(t.Context(), "source")
	if got.Exists || !errors.Is(err, chronicle.ErrProtocol) {
		t.Fatalf("foreign tail: %+v %v", got, err)
	}
	for _, call := range k.calls {
		if call == "sink" {
			t.Fatal("read foreign sink")
		}
	}
}
func TestMaterializedScenarioRefusesUnavailableObserverPosition(t *testing.T) {
	f, k := materializedFixture(t)
	if err := f.scenario.Given(t.Context(), "source", subscriptionAdded{}); err != nil {
		t.Fatal(err)
	}
	k.position = uint64(events.Unavailable)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	got, err := f.scenario.Instances(ctx)
	if got != nil || !errors.Is(err, ErrMaterializationIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unavailable position: %+v %v", got, err)
	}
}
