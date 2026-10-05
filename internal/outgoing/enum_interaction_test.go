// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package outgoing_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/outgoing"
	"github.com/cratis/chronicle.go/serialization"
)

type editedEnum int32

var editedEnumHooks atomic.Uint64

func (editedEnum) MarshalJSON() ([]byte, error) { editedEnumHooks.Add(1); panic("enum JSON hook") }
func (editedEnum) MarshalText() ([]byte, error) { editedEnumHooks.Add(1); panic("enum text hook") }
func (editedEnum) String() string               { editedEnumHooks.Add(1); panic("enum string hook") }
func (editedEnum) IsZero() bool                 { editedEnumHooks.Add(1); panic("enum zero hook") }

func editedEnumCodecs(t *testing.T) *serialization.Codecs {
	t.Helper()
	before := editedEnumHooks.Load()
	t.Cleanup(func() {
		if editedEnumHooks.Load() != before {
			t.Error("registered enum invoked application hooks")
		}
	})
	codecs, err := serialization.NewCodecs(serialization.Enum(
		serialization.EnumMember[editedEnum]{Name: "One", Value: 1},
		serialization.EnumMember[editedEnum]{Name: "Two", Value: 2}))
	if err != nil {
		t.Fatal(err)
	}
	return codecs
}

func TestEnumSiblingPreservesOwnedConceptBytes(t *testing.T) {
	type rootPair struct {
		State  editedEnum
		First  borrowedConcept
		Second borrowedNumber
	}
	codecs := editedEnumCodecs(t)
	root, err := events.Define[rootPair](events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 3)
	data, err := (outgoing.Config{}).Encode(t.Context(), root.Descriptor(), rootPair{1, borrowedConcept{buffer, `"A"`}, borrowedNumber{buffer}}, false, 0)
	if err != nil || string(data) != `{"State":1,"First":"A","Second":999}` {
		t.Fatalf("root bytes changed: %s, %#v", data, err)
	}
	type nestedPair struct {
		State editedEnum
		Pair  borrowedPair
	}
	definition, err := events.Define[nestedPair](events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprint(replace), func(t *testing.T) {
			buffer := make([]byte, 3)
			pair := borrowedPair{borrowedConcept{buffer, `"A"`}, borrowedNumber{buffer}}
			config := outgoing.Config{Enrichers: []events.EventEnricher{func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
				if replace {
					return c.Set("Pair", pair)
				}
				return nil
			}}}
			value := nestedPair{State: 1, Pair: pair}
			if replace {
				value.Pair = borrowedPair{borrowedConcept{make([]byte, 3), `"B"`}, borrowedNumber{make([]byte, 3)}}
			}
			data, err := config.Encode(t.Context(), definition.Descriptor(), value, false, 0)
			if err != nil || string(data) != `{"State":1,"Pair":{"First":"A","Second":999}}` {
				t.Fatalf("nested bytes changed: %s, %#v", data, err)
			}
		})
	}
	// An ordinary nested pair is not permission for a nested enum.
	type nestedEnum struct{ Child struct{ State editedEnum } }
	if _, err := events.Define[nestedEnum](events.WithCodecs(codecs)); err == nil {
		t.Fatal("nested enum escaped placement refusal")
	}
}

type enumEditedEvent struct {
	State    editedEnum
	Optional *editedEnum
	Many     []editedEnum
}

func TestEnumSetOwnsTypedValuesAndPreservesNullableOmission(t *testing.T) {
	definition, err := events.Define[enumEditedEvent](events.WithCodecs(editedEnumCodecs(t)))
	if err != nil {
		t.Fatal(err)
	}
	for _, omit := range []bool{false, true} {
		t.Run(fmt.Sprint(omit), func(t *testing.T) {
			state, many := editedEnum(2), []editedEnum{2, 1}
			config := outgoing.Config{Enrichers: []events.EventEnricher{func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
				if err := c.Set("State", editedEnum(2)); err != nil {
					return err
				}
				if err := c.Set("Optional", &state); err != nil {
					return err
				}
				if err := c.Set("Many", many); err != nil {
					return err
				}
				state, many[0] = 99, 99
				if omit {
					if err := c.Set("Optional", (*editedEnum)(nil)); err != nil {
						return err
					}
					return c.Set("Many", []editedEnum(nil))
				}
				return nil
			}}}
			data, err := config.Encode(t.Context(), definition.Descriptor(), enumEditedEvent{State: 1}, false, 0)
			want := `{"State":2,"Optional":2,"Many":[2,1]}`
			if omit {
				want = `{"State":2}`
			}
			if err != nil || string(data) != want {
				t.Fatalf("got %s, %#v; want %s", data, err, want)
			}
		})
	}
}

type countedSetterConcept struct {
	calls    *int
	failure  error
	panicked bool
}

func (countedSetterConcept) ConceptValue() string { return "" }
func (v countedSetterConcept) MarshalJSON() ([]byte, error) {
	*v.calls++
	if v.panicked {
		panic(v.failure)
	}
	return []byte(`"value"`), v.failure
}
func (*countedSetterConcept) UnmarshalJSON([]byte) error  { panic("unexpected decoder") }
func (countedSetterConcept) MarshalText() ([]byte, error) { panic("unexpected text") }
func (*countedSetterConcept) UnmarshalText([]byte) error  { panic("unexpected decoder") }

func TestEnumSetFailureLatchesAndRetainsConceptPanicInBothOrders(t *testing.T) {
	type value struct {
		State editedEnum
		Value countedSetterConcept
	}
	definition, err := events.Define[value](events.WithCodecs(editedEnumCodecs(t)))
	if err != nil {
		t.Fatal(err)
	}
	for _, order := range []string{"enum only", "enum first", "panic first"} {
		t.Run(order, func(t *testing.T) {
			calls, hooks, later := 0, 0, 0
			config := outgoing.Config{Enrichers: []events.EventEnricher{func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
				invalid := func() {
					if err := c.Set("State", editedEnum(0)); err == nil {
						t.Error("undeclared zero accepted")
					}
					raw, _, err := c.Get("State")
					if err != nil || string(raw) != "1" {
						t.Error("failed setter partially published")
					}
				}
				panicking := func() { _ = c.Set("Value", countedSetterConcept{&calls, hostileCodecError{&hooks}, true}) }
				if order == "panic first" {
					panicking()
				}
				invalid()
				if order == "enum first" {
					panicking()
				}
				// A subsequent successful setter must not clear the failed operation.
				return c.Set("State", editedEnum(2))
			}, func(context.Context, events.TypeRef, *events.EventContent) error { later++; return nil }}}
			data, err := config.Encode(t.Context(), definition.Descriptor(), value{1, countedSetterConcept{calls: &calls}}, false, 4)
			var failure *events.PreparationError
			if !errors.As(err, &failure) || failure.ProviderIndex != 0 || failure.EventIndex != 4 || failure.Panicked != (order != "enum only") || data != nil {
				t.Fatalf("failure=%#v data=%s", err, data)
			}
			wantCalls := 2
			if order == "enum only" {
				wantCalls = 1
			}
			if calls != wantCalls || hooks != 0 || later != 0 {
				t.Fatalf("codec=%d hooks=%d later=%d", calls, hooks, later)
			}
		})
	}
}
