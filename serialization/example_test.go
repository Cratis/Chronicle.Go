// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"fmt"
	"reflect"

	"github.com/cratis/chronicle.go/serialization"
)

func ExampleNamingPolicy() {
	type LinkAdded struct {
		URLValue string
		Person   string
		ID       string `json:"Id"` // Explicit shared name for Go ID and C# Id.
	}
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		plan, err := serialization.Compile(reflect.TypeFor[LinkAdded](), policy)
		if err != nil {
			panic(err)
		}
		data, err := plan.Marshal(LinkAdded{URLValue: "/docs", Person: "Ada", ID: "1"})
		if err != nil {
			panic(err)
		}
		fmt.Println(string(data))
	}
	// Output:
	// {"URLValue":"/docs","Person":"Ada","Id":"1"}
	// {"URLValue":"/docs","person":"Ada","Id":"1"}
	// {"urlValue":"/docs","person":"Ada","Id":"1"}
}
