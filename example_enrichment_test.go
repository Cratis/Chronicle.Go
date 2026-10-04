// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
)

type DeploymentRecorded struct {
	Version    string `json:"version"`
	RecordedBy string `json:"recordedBy"`
}

// Requires a development kernel at localhost:35000. This example is compiled;
// kernel-backed behavior is exercised separately by the integration suite.
func ExampleWithEventEnrichers() {
	ctx := metadata.WithIdentity(context.Background(), identities.Identity{Subject: "deployment-service"})
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[DeploymentRecorded](registry); err != nil {
		fmt.Println(err)
		return
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry), chronicle.WithDevelopmentDefaults(),
		chronicle.WithEventEnrichers(func(ctx context.Context, _ events.TypeRef, content *events.EventContent) error {
			return content.Set("recordedBy", metadata.Identity(ctx).Subject)
		}),
		chronicle.WithRootCausation(metadata.RootCausation{ProgramIdentifier: "deployment-recorder"}),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			fmt.Println(err)
		}
	}()
	store, err := client.EventStore(ctx, "deployments")
	if err != nil {
		fmt.Println(err)
		return
	}
	result, err := store.EventLog().Append(ctx, "service-a", DeploymentRecorded{Version: "1.0"})
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := result.Err(); err != nil {
		fmt.Println(err)
		return
	}

	// Begin binds audit identity/correlation; Stage enriches once, Commit reuses it.
	unit, owner, err := transactions.Begin(ctx, store.EventLog())
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "service-b", Event: DeploymentRecorded{Version: "1.0"}}}); err != nil {
		fmt.Println(err)
		return
	}
	batch, err := owner.Commit(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := batch.Err(); err != nil {
		fmt.Println(err)
	}
}
