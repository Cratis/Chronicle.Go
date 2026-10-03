// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type olderDerivedEvent struct{ Member derivedfixtures.Member }
type currentDerivedEvent struct{ Member derivedfixtures.Member }

func TestDerivedDescriptorsFreezeOptionsProvidersAndHistoricalGenerations(t *testing.T) {
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	option := events.WithCodecs(codecs)
	*codecs = serialization.Codecs{} // mutation after option creation must not change it
	calls := 0
	provider := compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		calls++
		return compliance.Classification{}, nil
	})
	current, err := events.Define[currentDerivedEvent](option, events.WithGeneration(2), events.WithProtection(provider))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := events.DefineGeneration[olderDerivedEvent](current, 1, option, events.WithProtection(provider))
	if err != nil {
		t.Fatal(err)
	}
	frozenCalls := calls
	if calls == 0 {
		t.Fatal("provider not resolved")
	}
	previousDescriptor, err := previous.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	currentDescriptor, err := current.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := currentDescriptor.WithNamingPolicy(serialization.PreservePropertyNames); err != nil {
			t.Fatal(err)
		}
	}
	if calls != frozenCalls {
		t.Fatal("rebind executed provider")
	}
	catalog, err := events.NewCatalog(currentDescriptor, previousDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	old, ok := catalog.LookupRef(previous.Ref())
	if !ok || !old.IsHistorical() {
		t.Fatal("missing original generation")
	}
	var decoded olderDerivedEvent
	if err := old.Unmarshal([]byte(`{"member":{"count":42,"_derivedTypeId":"robot"}}`), &decoded); err != nil || !reflect.DeepEqual(decoded.Member, derivedfixtures.RobotValue{Count: 42}) {
		t.Fatalf("historical decode: %#v %v", decoded, err)
	}
	// A variant field is not an ordinary migration path: selection is not inferred.
	migration, err := events.DefineMigration(current, previous, events.Migration[currentDerivedEvent, olderDerivedEvent]{
		Upcast: func(b *events.MigrationBuilder[currentDerivedEvent, olderDerivedEvent]) {
			b.RenamedFrom("Member.Count", "Member.Count")
		},
		Downcast: func(*events.MigrationBuilder[olderDerivedEvent, currentDerivedEvent]) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.WithMigrations([]events.MigrationDeclaration{migration}, true); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("ambiguous variant migration accepted: %v", err)
	}
}
