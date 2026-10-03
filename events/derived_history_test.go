// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

func TestHistoricalDerivedCatalogRetainsOriginalDiscriminators(t *testing.T) {
	oldCodecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	newCodecs, err := serialization.NewCodecs(serialization.Derived[derivedfixtures.Member, *derivedfixtures.HumanValue]("human-v2"), serialization.Derived[derivedfixtures.Member, derivedfixtures.RobotValue]("robot-v2"))
	if err != nil {
		t.Fatal(err)
	}
	current, err := events.Define[currentDerivedEvent](events.WithGeneration(2), events.WithCodecs(newCodecs))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := events.DefineGeneration[olderDerivedEvent](current, 1, events.WithCodecs(oldCodecs))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(current.Descriptor(), previous.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	appended := events.Appended{Context: events.Context{EventType: previous.Ref()}, Content: json.RawMessage(`{"Member":{"count":7,"_derivedTypeId":"robot"}}`)}
	value, err := events.Decode[olderDerivedEvent](catalog, appended)
	if err != nil || value.Member != (derivedfixtures.RobotValue{Count: 7}) {
		t.Fatalf("historic: %#v %v", value, err)
	}
	var wrong currentDerivedEvent
	if err := current.Descriptor().Unmarshal(appended.Content, &wrong); !errors.Is(err, faults.ErrProtocol) {
		t.Fatalf("historical bytes silently reinterpreted: %v", err)
	}
}
