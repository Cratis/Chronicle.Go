// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package declarations_test

import (
	"testing"

	"github.com/cratis/chronicle.go/declarations"
)

func TestProjectionNodeAndKeyGrammar(t *testing.T) {
	for _, tag := range []string{
		`add(E,from=Amount);subtract(E);increment(E,key=value("total"));decrement(E);count(E);clear(E)`,
		`children(E,key=composite(account=AccountID,tenant=context(namespace),constant=value("all")),identified-by=id,parent-key=source);remove(R,key=ItemID,parent-key=OrderID)`,
		`nested;clear(E);join(J,on=CustomerID,from=Name);remove-join(R,key=ID)`,
		`every;all(context=occurred);every(from=Name)`,
		`set(E,from=Name,once=true);context(E,once=false,from=occurred);value(E,value="new",once=true)`,
		`children(E,key=ItemID,prepend=true);children(F,order-by=Position);children(G,order-by-descending=Name)`,
	} {
		ds, err := declarations.Parse(declarations.V1, tag)
		if err == nil {
			err = declarations.Validate(declarations.Model, ds)
		}
		if err != nil {
			t.Fatalf("%s: %v", tag, err)
		}
		if declarations.Validate(declarations.Event, ds) == nil {
			t.Fatal("model tag accepted on an event")
		}
	}
	for _, tag := range []string{
		`every(E)`, `all(from=Name,context=occurred)`, `join(J,key=ID)`, `nested(E)`, `clear(E,from=X)`,
		`children(E,key=composite())`, `children(E,key=composite(a=X,a=Y))`, `children(E,key=context())`,
		`children(E,key=value(null))`, `remove-join(E,parent-key=ID)`, `children(E,identified-by=source())`,
		`set(E,once=yes)`, `set(E,once=true,once=false)`, `set(E,from=A,from=B)`, `value(E,once=true)`,
		`add(E,once=true)`, `children(E,prepend=yes)`, `children(E,order-by=value("x"))`, `remove(E,order-by=Name)`,
	} {
		ds, err := declarations.Parse(declarations.V1, tag)
		if err == nil {
			err = declarations.Validate(declarations.Model, ds)
		}
		if err == nil {
			t.Fatalf("accepted %s", tag)
		}
	}
}
