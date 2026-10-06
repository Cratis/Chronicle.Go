//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/projections"
	"github.com/google/uuid"
)

func TestKernelReadModelScenarioMaterializedStrictWithDefaults(t *testing.T) {
	r := eventRegistry(t)
	if _, err := chronicle.RegisterEvent[auditMarker](r); err != nil {
		t.Fatal(err)
	}
	m, err := chronicle.RegisterReadModel[ProjectedAccount](r)
	if err != nil {
		t.Fatal(err)
	}
	d := projections.ModelBound(m, projections.WithInitialValues(ProjectedAccount{Name: "default"}))
	ctx := kernelContext(t)
	config := kernelConfig(r)
	config.Store = chronicle.StoreName("ms-" + uuid.NewString()[:16])
	config.Namespace = chronicle.Namespace("ns-" + uuid.NewString()[:16])
	if config.ConnectionString == "" {
		t.Skip("set CHRONICLE_INTEGRATION_CONNECTION_STRING")
	}
	s, err := chronicletest.OpenReadModelScenario[ProjectedAccount](ctx, config, chronicletest.ReadModelOptions[ProjectedAccount]{Projection: &d, Materialized: true, StrictEventSubscription: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err = s.Given(ctx, "source", auditMarker{}); !errors.Is(err, chronicletest.ErrUnsubscribedEventSeeded) {
		t.Fatal(err)
	}
	if err = s.Given(ctx, "source", AccountOpened{Name: "event overrides default"}); err != nil {
		t.Fatal(err)
	}
	got, err := s.InstanceFor(ctx, "source")
	if err != nil || !got.Exists || got.Value.Name != "event overrides default" {
		t.Fatalf("strict defaults: %+v %v", got, err)
	}
	if err = s.Fidelity().Require(chronicletest.ReadModelStorage, chronicletest.ProjectionExecution); err != nil {
		t.Fatal(err)
	}
}
