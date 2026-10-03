// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// The integrations example prepares definitions without a kernel or DI container.
// It does not activate webhooks or capture external data.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/captures"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventstoresubscriptions"
	"github.com/cratis/chronicle.go/externalservices"
	"github.com/cratis/chronicle.go/webhooks"
)

type OrderPlaced struct{ OrderNumber string }

func run() (err error) {
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[OrderPlaced](registry, events.WithID("OrderPlaced"), events.WithSourceStore("orders"))
	if err != nil {
		return err
	}
	if err = chronicle.RegisterReactorHandler(registry, "shipping", func(context.Context, OrderPlaced) error { return nil }); err != nil {
		return err
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	artifacts, err := client.Artifacts("shipping")
	if err != nil {
		return err
	}
	subscription, err := eventstoresubscriptions.Define(artifacts.Events, "orders", "orders", event.Descriptor().Ref().ID)
	if err != nil {
		return err
	}
	webhook, err := webhooks.Define(artifacts.Events, "notify", "https://example.invalid/events", webhooks.WithActive(false))
	if err != nil {
		return err
	}
	service, err := externalservices.Define("OrdersApi", externalservices.HTTP("https://example.invalid"))
	if err != nil {
		return err
	}
	capture, err := new(captures.Builder).From(captures.API("OrdersApi", "/orders", "5m")).Key("id").Append(captures.Append(event, captures.Added(), map[string]string{"OrderNumber": "$.number"})).Build("ImportOrders")
	if err != nil {
		return err
	}
	fmt.Printf("observer=%s subscription=%s webhook=%s service=%s\n", artifacts.Reactors[0].EventSequence(), subscription.Identifier(), webhook.Identifier(), service.Name())
	fmt.Print(capture.Declaration())
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
