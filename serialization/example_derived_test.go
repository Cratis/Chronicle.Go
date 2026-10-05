// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type Actor interface{ actor() }
type Human struct{ Name string }

func (*Human) actor() {}

type Robot struct{ Model string }

func (Robot) actor() {}

type ActorsChanged struct{ Actors []Actor }
type ActorView struct {
	ID     string
	Actors []Actor
}

func ExampleDerived() {
	codecs, err := serialization.NewCodecs(
		serialization.Derived[Actor, *Human]("human"),
		serialization.Derived[Actor, Robot]("robot"),
	)
	if err != nil {
		panic(err)
	}
	event, err := events.Define[ActorsChanged](events.WithCodecs(codecs))
	if err != nil {
		panic(err)
	}
	model, err := readmodels.Define[ActorView](readmodels.WithCodecs(codecs))
	if err != nil {
		panic(err)
	}
	payload, err := event.Descriptor().Marshal(ActorsChanged{Actors: []Actor{&Human{Name: "Ada"}, Robot{Model: "R2"}}})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(payload))
	fmt.Println(len(model.Descriptor().Fields()[1].Derivatives()))
	// Output:
	// {"Actors":[{"name":"Ada","_derivedTypeId":"human"},{"model":"R2","_derivedTypeId":"robot"}]}
	// 2
}
