// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package seeding_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/seeding"
)

func TestSeedSourcesCannotUseReservedSubjects(t *testing.T) {
	type Person struct {
		Name string `chronicle:"pii"`
	}
	event, err := events.Define[Person]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, typed := range []bool{false, true} {
		definition, err := seeding.Prepare(catalog, seeding.Func(func(builder *seeding.Builder) error {
			builder.ForEventSource("ordinary", Person{Name: "fixture"})
			if typed {
				seeding.For(builder, "$chronicle-encrypted-value$namespace$", Person{Name: "fixture"})
			} else {
				builder.ForNamespace("tenant").ForEventSource("$chronicle-encrypted-value$namespace$", Person{Name: "fixture"})
			}
			return nil
		}))
		var subject *compliance.InvalidSubjectError
		if !errors.As(err, &subject) || !subject.Reserved || !definition.IsEmpty() {
			t.Fatalf("reserved seed subject admitted: %v", err)
		}
	}
}
