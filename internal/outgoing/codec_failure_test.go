// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package outgoing_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/outgoing"
)

type hostileCodecError struct{ hooks *int }

func (e hostileCodecError) Error() string { *e.hooks++; return "secret" }
func (e hostileCodecError) Is(error) bool { *e.hooks++; return true }
func (e hostileCodecError) As(any) bool   { *e.hooks++; return true }
func (e hostileCodecError) Unwrap() error { *e.hooks++; return nil }

type failingConcept struct {
	failure  error
	panicked bool
}

func (failingConcept) ConceptValue() string { return "value" }
func (v failingConcept) MarshalJSON() ([]byte, error) {
	if v.panicked {
		panic(v.failure)
	}
	if v.failure != nil {
		return nil, v.failure
	}
	return []byte(`"value"`), nil
}
func (*failingConcept) UnmarshalJSON([]byte) error  { panic("decoder must not run") }
func (failingConcept) MarshalText() ([]byte, error) { panic("not a JSON codec") }
func (*failingConcept) UnmarshalText([]byte) error  { panic("decoder must not run") }

type codecEnriched struct{ Value failingConcept }

func TestSetterCodecFailuresDiscardApplicationHooksAndPreservePanicFlag(t *testing.T) {
	definition, err := events.Define[codecEnriched]()
	if err != nil {
		t.Fatal(err)
	}
	for _, panicked := range []bool{false, true} {
		for _, ignored := range []bool{false, true} {
			hooks := 0
			config := outgoing.Config{Enrichers: []events.EventEnricher{func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
				err := c.Set("Value", failingConcept{hostileCodecError{&hooks}, panicked})
				if ignored {
					return nil
				}
				return err
			}}}
			data, err := config.Encode(t.Context(), definition.Descriptor(), codecEnriched{}, false, 2)
			var failure *events.PreparationError
			if !errors.As(err, &failure) || failure.Phase != "content" || failure.ProviderIndex != 0 || failure.EventIndex != 2 || failure.Panicked != panicked || hooks != 0 || data != nil {
				t.Fatalf("panic=%v ignored=%v error=%#v hooks=%d", panicked, ignored, err, hooks)
			}
		}
	}
}
