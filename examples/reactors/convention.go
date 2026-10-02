// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"context"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/reactors"
)

// ReservationGateway demonstrates an application-owned, idempotent external service.
type ReservationGateway interface {
	Reserve(context.Context, string, string) error
}

// begin-convention
// OrderConfirmed is the durable outcome of reserving an order's product.
type OrderConfirmed struct {
	Product string `json:"product"`
}

type ConfirmOrders struct {
	reservations ReservationGateway
}

// Reserve is discovered by OrderPlaced, not by its method name.
func (r *ConfirmOrders) Reserve(ctx context.Context, event OrderPlaced, delivery reactors.Delivery) (OrderConfirmed, error) {
	if err := r.reservations.Reserve(ctx, event.Product, delivery.ID()); err != nil {
		return OrderConfirmed{}, err
	}
	return OrderConfirmed(event), nil
}

func registerConfirmOrders(registry *chronicle.Registry, gateway ReservationGateway) error {
	if _, err := chronicle.RegisterEvent[OrderPlaced](registry); err != nil {
		return err
	}
	if _, err := chronicle.RegisterEvent[OrderConfirmed](registry); err != nil {
		return err
	}
	return chronicle.RegisterReactor[*ConfirmOrders](registry,
		func() *ConfirmOrders { return &ConfirmOrders{reservations: gateway} },
		reactors.WithID("confirm-orders"))
}

// end-convention
