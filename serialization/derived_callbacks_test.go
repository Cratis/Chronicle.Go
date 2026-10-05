// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

var errDerivedCallbackCause = errors.New("private-callback-content")

type derivedConcept string

func (derivedConcept) ConceptValue() string { panic("discovery must not call values") }
func (v derivedConcept) MarshalJSON() ([]byte, error) {
	if v == "panic" {
		panic("private-panic-content")
	}
	return nil, errDerivedCallbackCause
}
func (*derivedConcept) UnmarshalJSON(data []byte) error {
	if string(data) == `"panic"` || string(data) == "null" {
		panic("private-panic-content")
	}
	return errDerivedCallbackCause
}
func (derivedConcept) MarshalText() ([]byte, error) { return nil, errDerivedCallbackCause }
func (*derivedConcept) UnmarshalText([]byte) error  { return errDerivedCallbackCause }

type callbackVariant struct{ Value derivedConcept }
type zeroPanic string

func (zeroPanic) IsZero() bool { panic("private-zero-content") }

type zeroVariant struct {
	Value zeroPanic `json:",omitzero"`
}

type zeroFamily interface{ IsZero() bool }
type registeredZero struct{ Zero bool }

func (v *registeredZero) IsZero() bool { return v.Zero }

type unregisteredZero struct{}

func (unregisteredZero) IsZero() bool { return true }

type zeroFamilyEnvelope struct {
	Value zeroFamily `json:",omitzero"`
}

func TestDerivedOmitZeroCannotHideUnknownOrTypedNilVariants(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[zeroFamily, *registeredZero]("zero"))
	if err != nil {
		t.Fatal(err)
	}
	plan := derivedPlan[zeroFamilyEnvelope](t, codecs, 0)
	for _, value := range []zeroFamily{unregisteredZero{}, (*registeredZero)(nil)} {
		_, err := plan.Marshal(zeroFamilyEnvelope{Value: value})
		var panicked *serialization.CallbackPanicError
		if !errors.Is(err, faults.ErrUnsupported) || errors.As(err, &panicked) {
			t.Fatalf("omission bypassed family admission: %v", err)
		}
	}
	if data, err := plan.Marshal(zeroFamilyEnvelope{Value: &registeredZero{Zero: true}}); err != nil || string(data) != "{}" {
		t.Fatalf("admitted IsZero policy: %s %v", data, err)
	}
}

func TestDerivedCallbacksContainPanicsAndPreserveOrdinaryCauses(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Derived[any, callbackVariant]("callback"), serialization.Derived[any, zeroVariant]("zero"))
	if err != nil {
		t.Fatal(err)
	}
	plan := derivedPlan[derivedEnvelope](t, codecs, 0) // must not execute IsZero or ConceptValue
	for _, value := range []any{callbackVariant{"error"}, callbackVariant{"panic"}, zeroVariant{}} {
		_, err := plan.Marshal(derivedEnvelope{Value: value})
		var panicError *serialization.CallbackPanicError
		if !errors.Is(err, faults.ErrUnsupported) || strings.Contains(err.Error(), "private") {
			t.Fatalf("unsafe callback failure: %v", err)
		}
		if value == (callbackVariant{"error"}) {
			if !errors.Is(err, errDerivedCallbackCause) {
				t.Fatal("cause lost", err)
			}
		} else if !errors.As(err, &panicError) {
			t.Fatal("panic classification lost", err)
		}
	}
	for _, input := range []string{`"error"`, `"panic"`, "null"} {
		value := derivedEnvelope{Value: "unchanged"}
		err := plan.Unmarshal([]byte(`{"value":{"value":`+input+`,"_derivedTypeId":"callback"}}`), &value)
		if !errors.Is(err, faults.ErrProtocol) || strings.Contains(err.Error(), "private") || value.Value != "unchanged" {
			t.Fatalf("unsafe decode failure: %v %#v", err, value)
		}
		var panicError *serialization.CallbackPanicError
		if input == `"error"` {
			if !errors.Is(err, errDerivedCallbackCause) {
				t.Fatal("decode cause lost")
			}
		} else if !errors.As(err, &panicError) {
			t.Fatal("decode panic classification lost")
		}
	}
	for _, panics := range []bool{false, true} {
		_, err := plan.FreezeProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
			if panics {
				panic("private-provider-content")
			}
			return compliance.Classification{}, errDerivedCallbackCause
		}))
		if !errors.Is(err, faults.ErrInvalidConfiguration) || strings.Contains(err.Error(), "private") {
			t.Fatalf("unsafe provider failure: %v", err)
		}
		if !panics && !errors.Is(err, errDerivedCallbackCause) {
			t.Fatal("provider cause lost")
		}
	}
	// Snapshots rebind without invoking the runtime decoder or IsZero methods.
	next, err := plan.WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"value":{"value":"panic","_derivedTypeId":"callback"}}`)
	if _, err := plan.RebindJSON(data, next); err != nil {
		t.Fatal(err)
	}
	field, ok := serialization.FieldAt(plan.Fields(), "value")
	if !ok {
		t.Fatal("missing field")
	}
	if data, err := field.Marshal(nil); err != nil || string(data) != "null" {
		t.Fatalf("nil snapshot: %s %v", data, err)
	}
}
