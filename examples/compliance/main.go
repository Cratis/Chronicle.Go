// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// A disposable development example. It erases only a freshly generated subject
// in its own unique store. Never supply real personal data to a tutorial.
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
	"github.com/google/uuid"
)

type PersonRegistered struct {
	Name             string `json:"name" chronicle:"pii;compliance-details(value=\"contact\")"`
	OperationalValue string `json:"operationalValue" chronicle:"encrypted"`
}

func main() {
	if err := run(); err != nil {
		// Transport diagnostics can contain sensitive data. Do not print them.
		fmt.Fprintln(os.Stderr, "Compliance example failed; check the disposable kernel configuration.")
		os.Exit(1)
	}
}

func run() (err error) {
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		return errors.New("connection string required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[PersonRegistered](registry); err != nil {
		return err
	}
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	store, err := client.EventStore(ctx, chronicle.StoreName("go-pii-"+uuid.NewString()))
	if err != nil {
		return err
	}
	subject := events.SourceID(uuid.NewString())
	appendPerson := func() error {
		result, err := store.EventLog().Append(ctx, subject, PersonRegistered{Name: "Example", OperationalValue: "demonstration"})
		if err != nil {
			return err
		}
		return result.Err()
	}
	if err := appendPerson(); err != nil {
		return err
	}
	if err := store.Compliance().ErasePII(ctx, string(subject)); err != nil {
		return err
	}
	history, err := store.EventLog().ReadSource(ctx, subject, eventsequences.SourceFilter{})
	if err != nil {
		return err
	}
	if len(history) != 1 {
		return errors.New("expected one event")
	}
	value, err := events.Decode[PersonRegistered](store.EventTypes(), history[0])
	if err != nil {
		return err
	}
	if value.Name != "" || value.OperationalValue != "demonstration" {
		return errors.New("unexpected erasure result")
	}
	if err := appendPerson(); err == nil {
		return errors.New("write accepted after erasure")
	}
	if err := store.Compliance().AllowNewEncryptionKeyFor(ctx, string(subject)); err != nil {
		return err
	}
	if err := appendPerson(); err != nil {
		return err
	}
	fmt.Println("Personal data shredded; confidentiality retained; new writes explicitly authorized.")
	return nil
}
