// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestSubstituteCannotIssueDecisionEvidence(t *testing.T) {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[AccountOpened](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[AccountRenamed](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[Account](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err = chronicle.RegisterReducer[*AccountReducer](registry, model, func() *AccountReducer { return &AccountReducer{} }); err != nil {
		t.Fatal(err)
	}
	scenario, err := chronicletest.OpenReadModelScenario[Account](t.Context(), chronicletest.Config{Registry: registry, Engine: chronicletest.Substitute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scenario.Close(); err != nil {
			t.Error(err)
		}
	})
	read, err := readmodels.DecisionsFor(scenario.ReadModels(), model).GetDetached(t.Context(), "source")
	if !errors.Is(err, readmodels.ErrDecisionReadRefused) || !read.Token.IsZero() {
		t.Fatalf("substitute issued evidence: %+v %v", read, err)
	}
}
