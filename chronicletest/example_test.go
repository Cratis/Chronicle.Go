// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"context"
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
)

type AccountOpened struct{ Name string }
type AccountRenamed struct{ Name string }
type Account struct {
	ID   string `json:"id" chronicle:"key"`
	Name string
}
type AccountReducer struct{}

func (*AccountReducer) Opened(event AccountOpened, _ *Account) *Account {
	return &Account{Name: event.Name}
}
func (*AccountReducer) Renamed(event AccountRenamed, current *Account) *Account {
	if current == nil {
		return nil
	}
	return &Account{Name: event.Name}
}

type WelcomeRequested struct{ Message string }
type WelcomeReactor struct{}

func (*WelcomeReactor) Opened(event AccountOpened, account *Account) WelcomeRequested {
	return WelcomeRequested{Message: "Welcome " + event.Name + " to " + account.Name}
}

func ExampleEventScenario() {
	ctx := context.Background()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[AccountOpened](registry); err != nil {
		panic(err)
	}
	scenario, err := chronicletest.OpenEventScenario(ctx, chronicletest.Config{Registry: registry, Engine: chronicletest.Substitute})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := scenario.Close(); err != nil {
			panic(err)
		}
	}()
	if err := scenario.Given(ctx, "account-1", AccountOpened{Name: "Ada"}); err != nil {
		panic(err)
	}
	history, err := scenario.EventLog().ReadSource(ctx, "account-1", eventsequences.SourceFilter{})
	if err != nil {
		panic(err)
	}
	fmt.Println(len(history), history[0].Context.SequenceNumber)
	fmt.Println(scenario.Fidelity().Require(chronicletest.Constraints) != nil)
	// Output:
	// 1 0
	// true
}

func ExampleReadModelScenario() {
	ctx := context.Background()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[AccountOpened](registry); err != nil {
		panic(err)
	}
	if _, err := chronicle.RegisterEvent[AccountRenamed](registry); err != nil {
		panic(err)
	}
	model, err := chronicle.RegisterReadModel[Account](registry)
	if err != nil {
		panic(err)
	}
	if err := chronicle.RegisterReducer[*AccountReducer](registry, model, func() *AccountReducer { return &AccountReducer{} }); err != nil {
		panic(err)
	}
	scenario, err := chronicletest.OpenReadModelScenario[Account](ctx, chronicletest.Config{Registry: registry})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := scenario.Close(); err != nil {
			panic(err)
		}
	}()
	if err := scenario.Given(ctx, "account-1", AccountOpened{Name: "Ada"}, AccountRenamed{Name: "Grace"}); err != nil {
		panic(err)
	}
	instance, err := scenario.Instance(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(instance.Exists, instance.Value.Name)
	// Output: true Grace
}

func ExampleReactorScenario() {
	ctx := context.Background()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[AccountOpened](registry); err != nil {
		panic(err)
	}
	if _, err := chronicle.RegisterEvent[WelcomeRequested](registry); err != nil {
		panic(err)
	}
	if _, err := chronicle.RegisterReadModel[Account](registry, readmodels.WithIdentifier("Account")); err != nil {
		panic(err)
	}
	if err := chronicle.RegisterReactor[*WelcomeReactor](registry, func() *WelcomeReactor { return &WelcomeReactor{} }); err != nil {
		panic(err)
	}
	scenario, err := chronicletest.OpenReactorScenario[*WelcomeReactor](ctx, chronicletest.Config{Registry: registry})
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := scenario.Close(); err != nil {
			panic(err)
		}
	}()
	if err := scenario.SeedReadModel("account-1", Account{Name: "Chronicle"}); err != nil {
		panic(err)
	}
	if err := scenario.Given(ctx, "account-1", AccountOpened{Name: "Ada"}); err != nil {
		panic(err)
	}
	fmt.Println(scenario.Produced()[0].(WelcomeRequested).Message)
	// Output: Welcome Ada to Chronicle
}
