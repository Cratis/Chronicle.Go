// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"context"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

// begin-convention
type BalanceReducer struct{}

// Method names are descriptive; the first event parameter selects dispatch.
func (*BalanceReducer) Apply(ctx context.Context, event AmountChanged, current *Balance, ec events.Context) (*Balance, error) {
	return fold(ctx, event, current, ec)
}

func (*BalanceReducer) Delete(AccountDeleted, *Balance) *Balance { return nil }

func registerConvention(registry *chronicle.Registry, model readmodels.Model[Balance]) error {
	return chronicle.RegisterReducer[*BalanceReducer](registry, model,
		func() *BalanceReducer { return &BalanceReducer{} },
		reducers.WithID("account-balance"), reducers.WithVersion("1"))
}

// end-convention
