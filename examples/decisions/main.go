// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Command decisions demonstrates an admitted optimistic decision and one owned
// atomic commit. It creates a unique store on a disposable development kernel.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/transactions"
	"github.com/google/uuid"
)

type AccountNamed struct {
	Name string `json:"name"`
}
type Account struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(AccountNamed)"`
}

func rename(ctx context.Context, store *chronicle.EventStore, model readmodels.Model[Account], key readmodels.Key, name string) (result eventsequences.BatchResult, err error) {
	unit, owner, err := transactions.Begin(ctx, store.EventLog())
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, owner.Rollback()) }()

	reader := readmodels.DecisionsFor(store.ReadModels(), model)
	read, err := reader.Get(transactions.WithUnitOfWork(ctx, unit), key)
	if err != nil {
		return result, err
	}
	if !read.Instance.Exists || read.Instance.Value.Name != name {
		if err = unit.Stage(ctx, []eventsequences.Entry{{Source: events.SourceID(key), Event: AccountNamed{Name: name}}}); err != nil {
			return result, err
		}
	}
	// Even when no event was staged, an enrolled decision validates at commit.
	result, err = owner.Commit(ctx)
	if err != nil {
		return result, err
	}
	return result, result.Err()
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := chronicle.NewRegistry()
	if _, err = chronicle.RegisterEvent[AccountNamed](registry); err != nil {
		return err
	}
	model, err := chronicle.RegisterReadModel[Account](registry)
	if err != nil {
		return err
	}
	if err = registry.AddProjection(projections.ModelBound(model, projections.Passive())); err != nil {
		return err
	}
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		endpoint = "chronicle://localhost:35000"
	}
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	store, err := client.EventStore(ctx, chronicle.StoreName("go-decisions-"+uuid.NewString()))
	if err != nil {
		return err
	}
	first, err := rename(ctx, store, model, "account", "Example")
	if err != nil {
		return err
	}
	second, err := rename(ctx, store, model, "account", "Example")
	if err != nil {
		return err
	}
	if len(first.Positions) != 1 || len(second.Positions) != 0 || !first.ConcurrencyCheckPerformed || !second.ConcurrencyCheckPerformed {
		return errors.New("unexpected guarded commit result")
	}
	fmt.Println("Created account; unchanged decision validated without appending events")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
