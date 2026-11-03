// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/reducers"
)

type UserRegistered struct {
	Name string `json:"name"`
}

type UserRenamed struct {
	Name string `json:"name"`
}

type UserRemoved struct{}

// SearchIndex is an application-owned bulk API.
type SearchIndex interface {
	Bulk(ctx context.Context, operations []IndexOperation) (rejected []events.SourceID, throttled []events.SourceID, err error)
}

type IndexOperation struct {
	User   events.SourceID
	Name   string
	Delete bool
}

// Users indexes users with one bulk request per page.
type Users struct{ index SearchIndex }

func (u *Users) Index(ctx context.Context, batch *observation.EventBatch, _ observation.BatchContext) error {
	var operations []IndexOperation
	for _, item := range batch.All() {
		switch event := item.Event.(type) {
		case UserRegistered:
			operations = append(operations, IndexOperation{User: item.Context.SourceID, Name: event.Name})
		case UserRenamed:
			operations = append(operations, IndexOperation{User: item.Context.SourceID, Name: event.Name})
		case UserRemoved:
			operations = append(operations, IndexOperation{User: item.Context.SourceID, Delete: true})
		}
	}
	rejected, throttled, err := u.index.Bulk(ctx, operations)
	if err != nil {
		return err // Every partition in the batch fails at its first event.
	}
	for _, user := range rejected {
		if err := batch.Fail(user, errors.New("rejected by the index")); err != nil {
			return err
		}
	}
	for _, user := range throttled {
		if err := batch.Defer(user, 30*time.Second); err != nil {
			return err
		}
	}
	return nil
}

func registerUsers(registry *chronicle.Registry, index SearchIndex) error {
	return chronicle.RegisterReactor[*Users](registry, func() *Users { return &Users{index} },
		reactors.WithID("users-index"),
		reactors.WithBatchMethod("Index",
			observation.Observes[UserRegistered](),
			observation.Observes[UserRenamed](),
			observation.Observes[UserRemoved]()))
}

type fakeIndex struct{}

func (fakeIndex) Bulk(_ context.Context, operations []IndexOperation) ([]events.SourceID, []events.SourceID, error) {
	return nil, []events.SourceID{operations[len(operations)-1].User}, nil
}

func ExampleRegisterReactor_batchMethod() {
	registry := chronicle.NewRegistry()
	for _, register := range []func() error{
		func() error { _, err := chronicle.RegisterEvent[UserRegistered](registry); return err },
		func() error { _, err := chronicle.RegisterEvent[UserRenamed](registry); return err },
		func() error { _, err := chronicle.RegisterEvent[UserRemoved](registry); return err },
		func() error { return registerUsers(registry, fakeIndex{}) },
	} {
		if err := register(); err != nil {
			fmt.Println(err)
			return
		}
	}
	// A batch method is plain Go: call it with a batch to test it.
	batch := observation.NewEventBatch([]observation.EventAndContext{
		{Event: UserRegistered{Name: "Ada"}, Context: events.Context{SourceID: "ada", SequenceNumber: 1}},
		{Event: UserRegistered{Name: "Grace"}, Context: events.Context{SourceID: "grace", SequenceNumber: 2}},
	}, true)
	users := &Users{fakeIndex{}}
	if err := users.Index(context.Background(), batch, observation.BatchContext{}); err != nil {
		fmt.Println(err)
		return
	}
	for _, outcome := range batch.Outcomes() {
		fmt.Println(outcome.Partition, outcome.Kind == observation.OutcomeDeferred, outcome.RetryAfter)
	}
	// Output: grace true 30s
}

type Account struct {
	ID      string `json:"id"`
	Balance int    `json:"balance"`
}

type MoneyDeposited struct {
	Amount int `json:"amount"`
}

type MoneyWithdrawn struct {
	Amount int `json:"amount"`
}

// Accounts folds each account's events with one call per account.
type Accounts struct{}

func (Accounts) Reduce(batch *observation.EventBatch, current *Account) (*Account, error) {
	account := Account{}
	if current != nil {
		account = *current
	}
	for _, item := range batch.All() {
		account.ID = string(item.Context.SourceID)
		switch event := item.Event.(type) {
		case MoneyDeposited:
			account.Balance += event.Amount
		case MoneyWithdrawn:
			account.Balance -= event.Amount
		}
	}
	return &account, nil
}

func ExampleRegisterReducer_batchMethod() {
	registry := chronicle.NewRegistry()
	model, err := chronicle.RegisterReadModel[Account](registry)
	if err == nil {
		_, err = chronicle.RegisterEvent[MoneyDeposited](registry)
	}
	if err == nil {
		_, err = chronicle.RegisterEvent[MoneyWithdrawn](registry)
	}
	if err == nil {
		err = chronicle.RegisterReducer[Accounts](registry, model, nil,
			reducers.WithBatchMethod("Reduce",
				observation.Observes[MoneyDeposited](),
				observation.Observes[MoneyWithdrawn]()),
			reducers.WithMaxConcurrency(4))
	}
	if err != nil {
		fmt.Println(err)
		return
	}
	account, err := Accounts{}.Reduce(observation.NewEventBatch([]observation.EventAndContext{
		{Event: MoneyDeposited{Amount: 100}, Context: events.Context{SourceID: "acc-1", SequenceNumber: 1}},
		{Event: MoneyWithdrawn{Amount: 30}, Context: events.Context{SourceID: "acc-1", SequenceNumber: 2}},
	}, true), nil)
	fmt.Println(account.ID, account.Balance, err)
	// Output: acc-1 70 <nil>
}
