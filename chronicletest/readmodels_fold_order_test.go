// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/events"
)

func TestReducerScenarioFoldsInFirstSeenSourceOrderAndRetainsUnsubscribedInitialState(t *testing.T) {
	registry, _, numbers := reducerRegistry(t)
	initial := Account{Name: "initial"}
	s := chronicletest.NewReadModelScenario[Account](t, chronicletest.Config{Registry: registry}, chronicletest.ReadModelOptions[Account]{Initial: &initial})
	if err := s.Given(t.Context(), "b", auditMarker{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(t.Context(), "a", AccountOpened{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(t.Context(), "b", AccountOpened{Name: "Grace"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(t.Context(), "c", auditMarker{}); err != nil {
		t.Fatal(err)
	}
	values, err := s.Instances(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*numbers, []events.SequenceNumber{2, 1}) {
		t.Fatalf("fold order = %v, want [2 1]", *numbers)
	}
	if len(values) != 3 || values["a"].Name != "Ada" || values["b"].Name != "Grace" || values["c"].Name != "initial" {
		t.Fatalf("folded values = %+v", values)
	}
}
