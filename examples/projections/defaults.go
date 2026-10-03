// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

// projection-defaults:start
type StockRegistered struct {
	Name string `json:"name"`
}

type Stock struct {
	ID        string   `json:"id"`
	Name      string   `json:"name" chronicle:"set(StockRegistered)"`
	Available int32    `json:"available"`
	Locations []string `json:"locations"`
	Note      *string  `json:"note"`
}

func stockDeclarations() (*chronicle.Registry, readmodels.Model[Stock], error) {
	registry := chronicle.NewRegistry()
	registered, err := chronicle.RegisterEvent[StockRegistered](registry)
	if err != nil {
		return nil, readmodels.Model[Stock]{}, err
	}
	model, err := chronicle.RegisterReadModel[Stock](registry)
	if err != nil {
		return nil, model, err
	}
	declaration := projections.ModelBound(model,
		projections.FromEvent(registered),
		projections.WithInitialValues(Stock{Available: 10, Locations: []string{}}),
		projections.WithInitialValue(projections.Path[Stock, *string]("note"), nil),
		projections.WithLabels("inventory", "warehouse", "inventory"))
	return registry, model, registry.AddProjection(declaration)
}

// projection-defaults:end
