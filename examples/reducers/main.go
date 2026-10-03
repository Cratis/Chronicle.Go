// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// A plain-Go passive reducer. Requires a development kernel; no DI dependency.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type AmountChanged struct {
	Amount int `json:"amount"`
}
type AccountDeleted struct{}
type Balance struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

// The rule branches on prior state, unlike a projection counter: recovering a
// negative balance credits only half of a positive adjustment.
func fold(_ context.Context, event AmountChanged, current *Balance, ec events.Context) (*Balance, error) {
	amount := event.Amount
	if current != nil {
		if current.Amount < 0 && amount > 0 {
			amount /= 2
		}
		amount += current.Amount
	}
	return &Balance{ID: string(ec.SourceID), Amount: amount}, nil
}

func registerPlain(registry *chronicle.Registry) (readmodels.Model[Balance], error) {
	if _, err := chronicle.RegisterEvent[AmountChanged](registry); err != nil {
		return readmodels.Model[Balance]{}, err
	}
	if _, err := chronicle.RegisterEvent[AccountDeleted](registry); err != nil {
		return readmodels.Model[Balance]{}, err
	}
	model, err := chronicle.RegisterReadModel[Balance](registry)
	if err != nil {
		return model, err
	}
	// begin-plain
	err = chronicle.RegisterReducerHandlers(registry, model, "account-balance",
		[]reducers.Handler{
			reducers.On(fold),
			reducers.On(func(context.Context, AccountDeleted, *Balance, events.Context) (*Balance, error) {
				return nil, nil // Delete, not "ignore".
			}),
		}, reducers.Passive(), reducers.WithVersion("1"))
	// end-plain
	return model, err
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := chronicle.NewRegistry()
	model, err := registerPlain(registry)
	if err != nil {
		return err
	}
	endpoint := os.Getenv("CHRONICLE_CONNECTION_STRING")
	if endpoint == "" {
		endpoint = "chronicle://localhost:35000"
	}
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()
	namespace, err := metadata.NewCorrelationID()
	if err != nil {
		return err
	}
	store, err := client.EventStore(ctx, "plain-go-reducers", chronicle.WithNamespace(chronicle.Namespace(namespace.String())))
	if err != nil {
		return err
	}
	for _, amount := range []int{-10, 8} {
		result, err := store.EventLog().Append(ctx, "account", AmountChanged{Amount: amount})
		if err != nil {
			return err
		}
		if err := result.Err(); err != nil {
			return err
		}
	}
	state, err := readmodels.For(store.ReadModels(), model).Get(ctx, "account")
	if err != nil {
		return err
	}
	if !state.Exists || state.Value.Amount != -6 {
		return fmt.Errorf("unexpected balance: %+v", state)
	}
	fmt.Printf("Balance: %d\n", state.Value.Amount)
	return client.Close()
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
