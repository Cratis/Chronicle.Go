// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"context"
	"fmt"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/projections"
)

type MaterializedAccount struct {
	ID      string `json:"id" chronicle:"key"`
	Name    string
	Balance int32
}

// This example requires a running development kernel; it is compile-checked.
func ExampleReadModelScenario_materialized() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := chronicle.NewRegistry()
	opened, err := chronicle.RegisterEvent[AccountOpened](registry)
	if err != nil {
		panic(err)
	}
	model, err := chronicle.RegisterReadModel[MaterializedAccount](registry)
	if err != nil {
		panic(err)
	}
	builder := projections.NewBuilder("materialized-account", model,
		projections.WithInitialValues(MaterializedAccount{Name: "default", Balance: 41}))
	projections.From(builder, opened, func(from *projections.FromBuilder[MaterializedAccount, AccountOpened]) {
		projections.Increment(from, projections.Path[MaterializedAccount, int32]("Balance"))
	})
	declaration, err := builder.Build()
	if err != nil {
		panic(err)
	}
	if err = registry.AddProjection(declaration); err != nil {
		panic(err)
	}
	scenario, err := chronicletest.OpenReadModelScenario[MaterializedAccount](ctx, chronicletest.Config{
		Registry: registry, Engine: chronicletest.Kernel,
		ConnectionString: "chronicle://localhost:35000", Development: true,
	}, chronicletest.ReadModelOptions[MaterializedAccount]{Materialized: true, StrictEventSubscription: true})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := scenario.Close(); err != nil {
			panic(err)
		}
	}()
	if err = scenario.Given(ctx, "account-1", AccountOpened{Name: "not AutoMapped by aggregate-only mapping"}); err != nil {
		panic(err)
	}
	// Waits for observer-processing evidence, then reads the real sink once.
	// No default values are supplied by the fixture.
	instance, err := scenario.InstanceFor(ctx, "account-1")
	if err != nil {
		panic(err)
	}
	fmt.Println(instance.Exists, instance.Value.Name, instance.Value.Balance) // true default 42
}
