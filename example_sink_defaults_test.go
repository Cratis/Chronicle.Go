// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

type sinkExampleAccount struct{ ID, Name string }
type sinkExampleAudit struct{ ID, Message string }

func ExampleWithDefaultSinkType() {
	registry := chronicle.NewRegistry()
	account, err := chronicle.RegisterReadModel[sinkExampleAccount](registry)
	if err != nil {
		panic(err)
	}
	audit, err := chronicle.RegisterReadModel[sinkExampleAudit](registry,
		readmodels.WithSink(readmodels.Sink{Type: readmodels.MongoDB}))
	if err != nil {
		panic(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry),
		chronicle.WithDefaultSinkType(readmodels.SQL))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			panic(err)
		}
	}()
	_, models, err := client.Catalogs("accounts") // Offline: no database or RPC.
	if err != nil {
		panic(err)
	}
	inherited, _ := models.LookupIdentifier(account.Identifier())
	explicit, _ := models.LookupIdentifier(audit.Identifier())
	fmt.Println(inherited.Sink().Type, explicit.Sink().Type)
	// Registration alone does not create instances or configure a SQL backend.
	// Output: SQL MongoDB
}
