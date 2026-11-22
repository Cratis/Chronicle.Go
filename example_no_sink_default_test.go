// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type noSinkExampleRenamed struct{ Name string }
type noSinkExampleAccount struct {
	ID   string `chronicle:"key"`
	Name string
}

func ExampleWithDefaultSinkType_none() {
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[noSinkExampleRenamed](registry)
	if err != nil {
		panic(err)
	}
	model, err := chronicle.RegisterReadModel[noSinkExampleAccount](registry)
	if err != nil {
		panic(err)
	}
	// Ordinary source-keyed projection: no Passive option or custom key.
	if err := registry.AddProjection(projections.ModelBound(model, projections.FromEvent(event))); err != nil {
		panic(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithDefaultSinkType(readmodels.NoSink))
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()
	_, models, err := client.Catalogs("accounts") // Offline: no registration or RPC.
	if err != nil {
		panic(err)
	}
	resolved, _ := models.LookupIdentifier(model.Identifier())
	fmt.Println(resolved.Sink().Type, model.Descriptor().Sink().Type)
	// Connected Get reads replay by source identity; this is not a live window.
	// The original declaration remains unchanged.
	// Output: None MongoDB
}
