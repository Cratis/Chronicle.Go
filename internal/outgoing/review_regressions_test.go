// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package outgoing_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/outgoing"
)

type borrowedConcept struct {
	buffer []byte
	value  string
}

func (borrowedConcept) ConceptValue() string { return "" }
func (v borrowedConcept) MarshalJSON() ([]byte, error) {
	copy(v.buffer, v.value)
	return v.buffer, nil
}
func (*borrowedConcept) UnmarshalJSON([]byte) error  { panic("decoder must not run") }
func (borrowedConcept) MarshalText() ([]byte, error) { panic("text must not run") }
func (*borrowedConcept) UnmarshalText([]byte) error  { panic("decoder must not run") }

type borrowedNumber struct{ buffer []byte }

func (borrowedNumber) ConceptValue() int32            { return 999 }
func (v borrowedNumber) MarshalJSON() ([]byte, error) { copy(v.buffer, "999"); return v.buffer, nil }
func (*borrowedNumber) UnmarshalJSON([]byte) error    { panic("decoder must not run") }
func (borrowedNumber) MarshalText() ([]byte, error)   { panic("text must not run") }
func (*borrowedNumber) UnmarshalText([]byte) error    { panic("decoder must not run") }

type borrowedPair struct {
	First  borrowedConcept
	Second borrowedNumber
}
type borrowedEvent struct{ Pair borrowedPair }

func TestOutgoingOwnsConceptBytesBeforeNextCodec(t *testing.T) {
	root, err := events.Define[borrowedPair]()
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 3)
	data, err := (outgoing.Config{}).Encode(t.Context(), root.Descriptor(), borrowedPair{borrowedConcept{buffer, `"A"`}, borrowedNumber{buffer}}, false, 0)
	if err != nil || string(data) != `{"First":"A","Second":999}` {
		t.Fatalf("root codec bytes changed content: %s, %#v", data, err)
	}
	definition, err := events.Define[borrowedEvent]()
	if err != nil {
		t.Fatal(err)
	}
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			buffer := make([]byte, 3)
			pair := borrowedPair{borrowedConcept{buffer, `"A"`}, borrowedNumber{buffer}}
			config := outgoing.Config{Enrichers: []events.EventEnricher{func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
				if replacement {
					return c.Set("Pair", pair)
				}
				return nil
			}}}
			value := borrowedEvent{Pair: pair}
			if replacement {
				value = borrowedEvent{Pair: borrowedPair{borrowedConcept{make([]byte, 3), `"A"`}, borrowedNumber{make([]byte, 3)}}}
			}
			data, err := config.Encode(t.Context(), definition.Descriptor(), value, false, 0)
			if err != nil || string(data) != `{"Pair":{"First":"A","Second":999}}` {
				t.Fatalf("borrowed bytes changed content: %s, %#v", data, err)
			}
		})
	}
}

func TestBaseCodecWithoutEnrichersDiscardsApplicationErrors(t *testing.T) {
	definition, err := events.Define[codecEnriched]()
	if err != nil {
		t.Fatal(err)
	}
	for _, identityOnly := range []bool{false, true} {
		for _, panicked := range []bool{false, true} {
			hooks := 0
			config := outgoing.Config{}
			if identityOnly {
				config.Identity = func(context.Context) (identities.Identity, bool, error) {
					return identities.Identity{Subject: "actor"}, true, nil
				}
			}
			audit, err := config.Resolve(t.Context(), [16]byte{}, false, [16]byte{})
			if err != nil {
				t.Fatal(err)
			}
			applicationError := hostileCodecError{&hooks}
			data, err := config.Encode(audit.Context(t.Context()), definition.Descriptor(), codecEnriched{failingConcept{applicationError, panicked}}, false, 3)
			var failure *events.PreparationError
			if !errors.As(err, &failure) || failure.Phase != "content" || failure.ProviderIndex != -1 || failure.EventIndex != 3 || failure.Panicked != panicked || data != nil {
				t.Fatalf("identity=%t panic=%t error=%#v", identityOnly, panicked, err)
			}
			_ = fmt.Sprint(err)
			if errors.Is(err, errors.New("innocent")) || errors.Is(err, applicationError) || hooks != 0 {
				t.Fatalf("application error retained or inspected: hooks=%d", hooks)
			}
		}
	}
}

func TestIgnoredContentFailuresPreservePanicEvidenceInBothOrders(t *testing.T) {
	definition, err := events.Define[codecEnriched]()
	if err != nil {
		t.Fatal(err)
	}
	for _, panicFirst := range []bool{false, true} {
		hooks := 0
		config := outgoing.Config{Enrichers: []events.EventEnricher{func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
			ordinary := func() { _ = c.Remove("Value") }
			panicking := func() { _ = c.Set("Value", failingConcept{hostileCodecError{&hooks}, true}) }
			if panicFirst {
				panicking()
				ordinary()
			} else {
				ordinary()
				panicking()
			}
			return nil
		}}}
		data, err := config.Encode(t.Context(), definition.Descriptor(), codecEnriched{}, false, 4)
		var failure *events.PreparationError
		if !errors.As(err, &failure) || !failure.Panicked || failure.EventIndex != 4 || failure.ProviderIndex != 0 || hooks != 0 || data != nil {
			t.Fatalf("panicFirst=%t error=%#v hooks=%d", panicFirst, err, hooks)
		}
	}
}
